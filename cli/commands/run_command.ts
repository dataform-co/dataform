import yargs from "yargs";

import { build, compile, credentials, run, test } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { createLineageEmitter as createLineageEmitterFromFactory } from "df/cli/api/lineage/emitter_factory";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  actionsOption,
  coerceTimeout,
  credentialsOption,
  jsonOutputOption,
  projectDirMustExistOption,
  projectDirOption,
  quietCompileOption,
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
import { ProjectConfigOptions } from "df/cli/project_config_options";
import { actuallyResolve, compiledGraphHasErrors } from "df/cli/util";
import { ICommand, INamedOption } from "df/cli/yargswrapper";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";

// Maximum time to wait for outstanding lineage emissions to complete before
// `dataform run` returns. Lineage emission is fail-open — if we don't drain
// within this window, in-flight requests are abandoned and the run status is
// unaffected.
const LINEAGE_DRAIN_TIMEOUT_MS = 15_000;

const fullRefreshOption: INamedOption<yargs.Options> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

const tagsOption: INamedOption<yargs.Options> = {
  name: "tags",
  option: {
    describe: "A list of tags to filter the actions to run.",
    type: "array",
    coerce: splitCommas
  }
};

const includeDepsOption: INamedOption<yargs.Options> = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-deps", actionsOption, tagsOption)
};

const includeDependentsOption: INamedOption<yargs.Options> = {
  name: "include-dependents",
  option: {
    describe: "If set, dependents (downstream) for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-dependents", actionsOption, tagsOption)
};

const emitLineageOption: INamedOption<yargs.Options> = {
  name: "emit-lineage",
  option: {
    describe:
      "If set, emit OpenLineage RunEvents to Knowledge Catalog Lineage for each executed action. " +
      "Overrides workflow_settings.yaml lineage.enabled when specified.",
    type: "boolean"
  }
};

const executionTimeoutOption: INamedOption<yargs.Options> = {
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

const jitTimeoutOption: INamedOption<yargs.Options> = {
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

const jobPrefixOption: INamedOption<yargs.Options> = {
  name: "job-prefix",
  option: {
    describe: "Adds an additional prefix in the form of `dataform-${jobPrefix}-`.",
    type: "string",
    default: null
  }
};

const bigqueryJobLabelsOption: INamedOption<yargs.Options> = {
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

const dryRunOptionName = "dry-run";
const runTestsOptionName = "run-tests";

const actionRetryLimitName = "action-retry-limit";

function createLineageEmitter(
  argv: yargs.Arguments<any>,
  executionGraph: dataform.IExecutionGraph,
  readCredentials: dataform.IBigQuery | undefined
): LineageEmitter | undefined {
  return createLineageEmitterFromFactory({
    cliEmitLineage: argv[emitLineageOption.name] as boolean | undefined,
    workflowLineageEnabled: executionGraph.projectConfig?.lineageEnabled ?? undefined,
    dryRun: !!argv[dryRunOptionName],
    projectDir: argv[projectDirOption.name] || process.cwd(),
    readCredentials
  });
}

export const runCommand: ICommand = {
  format: `run [${projectDirMustExistOption.name}]`,
  description: "Run the dataform project.",
  positionalOptions: [projectDirMustExistOption],
  options: [
    {
      name: dryRunOptionName,
      option: {
        describe:
          "If set, BigQuery will validate the run SQL without applying changes to the warehouse.",
        type: "boolean"
      }
    },
    {
      name: runTestsOptionName,
      option: {
        describe:
          "If set, the project's unit tests are required to pass before running the project.",
        type: "boolean"
      }
    },
    {
      name: actionRetryLimitName,
      option: {
        describe: "If set, idempotent actions will be retried up to the limit.",
        type: "number",
        default: 0
      }
    },
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
    const isJsonOutput = argv[jsonOutputOption.name];
    const logger = new Logger(!isJsonOutput);

    if (isJsonOutput && !argv[dryRunOptionName]) {
      printError(
        `For execution, the --${jsonOutputOption.name} option is only supported if the ` +
          `--${dryRunOptionName} option is enabled`
      );
      return 1;
    }
    if (
      !isJsonOutput &&
      argv[timeoutOption.name] != null &&
      argv[executionTimeoutOption.name] == null
    ) {
      printWarning(
        "Note: --timeout only bounds project compilation. " +
          "For a whole-run wall-clock deadline, use --execution-timeout.\n"
      );
    }
    logger.log("Compiling...\n");
    const compiledGraph = await compile({
      projectDir: argv[projectDirOption.name],
      projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
      timeoutMillis: argv[timeoutOption.name] || undefined
    });
    if (compiledGraphHasErrors(compiledGraph)) {
      printCompiledGraphErrors(compiledGraph.graphErrors, argv[quietCompileOption.name]);
      return 1;
    }
    logger.success("Compiled successfully.\n");
    const readCredentials = credentials.read(
      actuallyResolve(argv[projectDirOption.name], argv[credentialsOption.name])
    );

    const dbadapter = new BigQueryDbAdapter(readCredentials);
    const executionGraph = await build(
      compiledGraph,
      {
        fullRefresh: argv[fullRefreshOption.name],
        actions: argv[actionsOption.name],
        includeDependencies: argv[includeDepsOption.name],
        includeDependents: argv[includeDependentsOption.name],
        tags: argv[tagsOption.name],
        timeoutMillis: argv[executionTimeoutOption.name] || undefined,
        jitTimeoutMillis: argv[jitTimeoutOption.name] || undefined
      },
      dbadapter
    );

    if (
      argv[dryRunOptionName] &&
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

    if (argv[runTestsOptionName]) {
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
      actionRetryLimit: argv[actionRetryLimitName]
    };
    if (argv[dryRunOptionName]) {
      bigqueryOptions = { ...bigqueryOptions, dryRun: argv[dryRunOptionName] };
    }
    if (argv[jobPrefixOption.name]) {
      bigqueryOptions = { ...bigqueryOptions, jobPrefix: argv[jobPrefixOption.name] };
    }
    if (argv[bigqueryJobLabelsOption.name]) {
      bigqueryOptions = { ...bigqueryOptions, labels: argv[bigqueryJobLabelsOption.name] };
    }

    const actionsByName = new Map<string, dataform.IExecutionAction>();
    executionGraph.actions.forEach(action => {
      actionsByName.set(targetAsReadableString(action.target), action);
    });

    if (actionsByName.size === 0) {
      logger.log("No actions to run.\n");
      return 0;
    }

    if (argv[dryRunOptionName]) {
      logger.log("Dry running (no changes to the warehouse will be applied)...");
    } else {
      logger.log("Running...\n");
    }

    const lineageEmitter = createLineageEmitter(argv, executionGraph, readCredentials);

    const runner = run(
      dbadapter,
      executionGraph,
      {
        projectDir: argv[projectDirOption.name],
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
            argv[dryRunOptionName]
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
        const executionTimeoutMillis = argv[executionTimeoutOption.name];
        const suffix = executionTimeoutMillis
          ? ` after ${executionTimeoutMillis / 1000} seconds (--execution-timeout)`
          : "";
        printError(`Run timed out${suffix}.`);
      } else if (runResult.status === dataform.RunResult.ExecutionStatus.CANCELLED) {
        printError("Run cancelled.");
      }
    }
    return runResult.status === dataform.RunResult.ExecutionStatus.SUCCESSFUL ? 0 : 1;
  }
};
