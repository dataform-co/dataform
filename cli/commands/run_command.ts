import yargs from "yargs";

import { build, compile, credentials, run, test } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { createLineageEmitter as createLineageEmitterFromFactory } from "df/cli/api/lineage/emitter_factory";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  dryRunOption,
  executionTimeoutOption,
  IRunArgs,
  runOptions
} from "df/cli/commands/run_options";
import {
  assertProjectDirExists,
  jsonOutputOption,
  projectDirOption
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
import { ICommand } from "df/cli/yargswrapper";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";

// Maximum time to wait for outstanding lineage emissions to complete before
// `dataform run` returns. Lineage emission is fail-open — if we don't drain
// within this window, in-flight requests are abandoned and the run status is
// unaffected.
const LINEAGE_DRAIN_TIMEOUT_MS = 15_000;

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
  options: runOptions,
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
      return 0;
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
