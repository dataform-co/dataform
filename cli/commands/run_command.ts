import yargs from "yargs";

import { build, compile, credentials, run, test } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { createLineageEmitter as createLineageEmitterFromFactory } from "df/cli/api/lineage/emitter_factory";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  actionsOption,
  assertProjectDirExists,
  coerceTimeout,
  credentialsOption,
  IActionsArgs,
  ICredentialsArgs,
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  projectDirOption,
  requiresSelection,
  splitCommas,
  timeoutOption
} from "df/cli/common_options";
import {
  Logger,
  print,
  printCompiledGraphErrors,
  printError,
  printExecutedAction,
  printExecutionGraph,
  printTestResult,
  printWarning
} from "df/cli/console";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { actuallyResolve, compiledGraphHasErrors } from "df/cli/util";
import { ICommand, INamedOption } from "df/cli/yargswrapper";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";

// Maximum time to wait for outstanding lineage emissions to complete before
// `dataform run` returns. Lineage emission is fail-open — if we don't drain
// within this window, in-flight requests are abandoned and the run status is
// unaffected.
const LINEAGE_DRAIN_TIMEOUT_MS = 15_000;

export interface IRunArgs
  extends IProjectDirArgs,
    IProjectConfigArgs,
    ICredentialsArgs,
    IActionsArgs,
    IJsonOutputArgs,
    ITimeoutArgs {
  dryRun?: boolean;
  runTests?: boolean;
  actionRetryLimit: number;
  emitLineage?: boolean;
  fullRefresh: boolean;
  includeDeps?: boolean;
  includeDependents?: boolean;
  executionTimeout?: number | null;
  jitTimeout?: number | null;
  jobPrefix?: string | null;
  tags?: string[];
  jobLabels?: { [key: string]: string };
}

const dryRunOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "dry-run",
  option: {
    describe:
      "If set, BigQuery will validate the run SQL without applying changes to the warehouse.",
    type: "boolean"
  }
};

const runTestsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "run-tests",
  option: {
    describe:
      "If set, the project's unit tests are required to pass before running the project.",
    type: "boolean"
  }
};

const actionRetryLimitOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "action-retry-limit",
  option: {
    describe: "If set, idempotent actions will be retried up to the limit.",
    type: "number",
    default: 0
  }
};

const fullRefreshOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

const tagsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "tags",
  option: {
    describe: "A list of tags to filter the actions to run.",
    type: "array",
    coerce: splitCommas
  }
};

const includeDepsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-deps", actionsOption, tagsOption)
};

const includeDependentsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-dependents",
  option: {
    describe: "If set, dependents (downstream) for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-dependents", actionsOption, tagsOption)
};

const emitLineageOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "emit-lineage",
  option: {
    describe:
      "If set, emit OpenLineage RunEvents to Knowledge Catalog Lineage for each executed action. " +
      "Overrides workflow_settings.yaml lineage.enabled when specified.",
    type: "boolean"
  }
};

const executionTimeoutOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "execution-timeout",
  option: {
    describe:
      "Wall-clock deadline for the entire run (compile + all actions). When it fires, " +
      "in-flight actions are cancelled and pending actions are skipped. Off by default. " +
      "Examples: '10m', '2h'.",
    type: "string",
    default: null,
    coerce: coerceTimeout
  }
};

const jitTimeoutOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "jit-timeout",
  option: {
    describe:
      "Per-model JiT compilation worker timeout. Each action with jitCode gets " +
      "its own fresh deadline; independent of --execution-timeout. When unset, no " +
      "per-model cap is applied and only --execution-timeout bounds JiT work. " +
      "Examples: '30s', '2m'.",
    type: "string",
    default: null,
    coerce: coerceTimeout
  }
};

const jobPrefixOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-prefix",
  option: {
    describe: "Adds an additional prefix in the form of `dataform-${jobPrefix}-`.",
    type: "string",
    default: null
  }
};

const bigqueryJobLabelsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-labels",
  option: {
    describe:
      "Comma-separated list of labels to add to BigQuery jobs, e.g. 'key1=val1,key2=val2'.",
    type: "string",
    coerce: (raw: string | null) => {
      const labels: { [key: string]: string } = {};
      raw?.split(",").forEach(kv => {
        if (!kv) {
          return;
        }
        const [key, ...rest] = kv.split("=");
        labels[key] = rest.join("=") || "";
      });
      return labels;
    }
  }
};

function createLineageEmitter(
  argv: yargs.Arguments<IRunArgs>,
  executionGraph: dataform.IExecutionGraph,
  readCredentials: dataform.IBigQuery | undefined
): LineageEmitter | undefined {
  return createLineageEmitterFromFactory({
    cliEmitLineage: argv.emitLineage,
    workflowLineageEnabled: executionGraph.projectConfig?.lineageEnabled ?? undefined,
    dryRun: !!argv.dryRun,
    projectDir: argv.projectDir || process.cwd(),
    readCredentials
  });
}

export const runCommand: ICommand<IRunArgs> = {
  format: `run [${projectDirOption.name}]`,
  description: "Run the dataform project.",
  positionalOptions: [projectDirOption],
  check: [assertProjectDirExists],
  options: [
    dryRunOption,
    runTestsOption,
    actionRetryLimitOption,
    actionsOption,
    credentialsOption,
    emitLineageOption,
    fullRefreshOption,
    includeDepsOption,
    includeDependentsOption,
    jsonOutputOption,
    timeoutOption,
    executionTimeoutOption,
    jitTimeoutOption,
    jobPrefixOption,
    tagsOption,
    bigqueryJobLabelsOption,
    ...ProjectConfigOptions.allYargsOptions
  ],
  processFn: async argv => {
    const isJsonOutput = argv.json;
    const logger = new Logger(!isJsonOutput);

    if (isJsonOutput && !argv.dryRun) {
      printError(
        `For execution, the --${jsonOutputOption.name} option is only supported if the ` +
          `--${dryRunOption.name} option is enabled`
      );
      return 1;
    }
    if (
      !isJsonOutput &&
      argv.timeout != null &&
      argv.executionTimeout == null
    ) {
      printWarning(
        "Note: --timeout only bounds project compilation. " +
          "For a whole-run wall-clock deadline, use --execution-timeout.\n"
      );
    }
    logger.log("Compiling...\n");
    const compiledGraph = await compile({
      projectDir: argv.projectDir,
      projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
      timeoutMillis: argv.timeout || undefined
    });
    if (compiledGraphHasErrors(compiledGraph)) {
      printCompiledGraphErrors(compiledGraph.graphErrors);
      return 1;
    }
    logger.success("Compiled successfully.\n");
    const readCredentials = credentials.read(
      actuallyResolve(argv.projectDir, argv.credentials)
    );

    const dbadapter = new BigQueryDbAdapter(readCredentials);
    const executionGraph = await build(
      compiledGraph,
      {
        fullRefresh: argv.fullRefresh,
        actions: argv.actions,
        includeDependencies: argv.includeDeps,
        includeDependents: argv.includeDependents,
        tags: argv.tags,
        timeoutMillis: argv.executionTimeout || undefined,
        jitTimeoutMillis: argv.jitTimeout || undefined
      },
      dbadapter
    );

    if (
      argv.dryRun &&
      isJsonOutput &&
      // Skip the early graph print when JiT actions are present: their compiled
      // SQL is only produced once the Runner triggers JiT compilation, so falling
      // through ensures the JSON dry-run output includes the generated SQL rather
      // than the raw jitCode.
      !executionGraph.actions.some(action => !!action.jitCode)
    ) {
      printExecutionGraph(executionGraph, isJsonOutput);
      return;
    }

    if (argv.runTests) {
      logger.log(`Running ${compiledGraph.tests.length} unit tests...\n`);
      const testResults = await test(dbadapter, compiledGraph.tests);
      testResults.forEach(testResult => printTestResult(testResult));
      if (testResults.some(testResult => !testResult.successful)) {
        printError("\nUnit tests did not pass; aborting run.");
        return 1;
      }
      logger.success("Unit tests completed successfully.\n");
    }

    let bigqueryOptions: {} = {
      actionRetryLimit: argv.actionRetryLimit
    };
    if (argv.dryRun) {
      bigqueryOptions = { ...bigqueryOptions, dryRun: argv.dryRun };
    }
    if (argv.jobPrefix) {
      bigqueryOptions = { ...bigqueryOptions, jobPrefix: argv.jobPrefix };
    }
    if (argv.jobLabels) {
      bigqueryOptions = { ...bigqueryOptions, labels: argv.jobLabels };
    }

    const actionsByName = new Map<string, dataform.IExecutionAction>();
    executionGraph.actions.forEach(action => {
      actionsByName.set(targetAsReadableString(action.target), action);
    });

    if (actionsByName.size === 0) {
      logger.log("No actions to run.\n");
      return 0;
    }

    if (argv.dryRun) {
      logger.log("Dry running (no changes to the warehouse will be applied)...");
    } else {
      logger.log("Running...\n");
    }

    const lineageEmitter = createLineageEmitter(argv, executionGraph, readCredentials);

    const runner = run(
      dbadapter,
      executionGraph,
      {
        projectDir: argv.projectDir,
        bigquery: bigqueryOptions,
        lineageEmitter
      }
    );
    process.on("SIGINT", () => {
      runner.cancel();
    });

    const alreadyPrintedActions = new Set<string>();

    const printExecutedGraph = (executedGraph: dataform.IRunResult) => {
      executedGraph.actions
        .filter(
          actionResult =>
            actionResult.status !== dataform.ActionResult.ExecutionStatus.RUNNING
        )
        .filter(
          executedAction =>
            !alreadyPrintedActions.has(targetAsReadableString(executedAction.target))
        )
        .forEach(executedAction => {
          printExecutedAction(
            executedAction,
            actionsByName.get(targetAsReadableString(executedAction.target)),
            argv.dryRun
          );
          alreadyPrintedActions.add(targetAsReadableString(executedAction.target));
        });
    };

    if (!isJsonOutput) {
      runner.onChange(printExecutedGraph);
    }
    const runResult = await runner.result();
    if (lineageEmitter) {
      await lineageEmitter.drain(LINEAGE_DRAIN_TIMEOUT_MS);
    }
    if (!isJsonOutput) {
      printExecutedGraph(runResult);
    }
    if (isJsonOutput) {
      print(prettyJsonStringify(runResult));
    }
    if (!isJsonOutput) {
      if (runResult.status === dataform.RunResult.ExecutionStatus.TIMED_OUT) {
        const executionTimeoutMillis = argv.executionTimeout;
        const suffix = executionTimeoutMillis
          ? ` after ${executionTimeoutMillis / 1000} seconds (--${executionTimeoutOption.name})`
          : "";
        printError(`Run timed out${suffix}.`);
      } else if (runResult.status === dataform.RunResult.ExecutionStatus.CANCELLED) {
        printError("Run cancelled.");
      }
    }
    return runResult.status === dataform.RunResult.ExecutionStatus.SUCCESSFUL ? 0 : 1;
  }
};
