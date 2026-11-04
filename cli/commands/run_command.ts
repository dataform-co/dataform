import yargs from "yargs";

import { build, credentials, run, test } from "df/cli/api";
import { compileForRun } from "df/cli/api/commands/compile";
import { checkJobLabelLimit } from "df/cli/api/commands/run";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { createLineageEmitter as createLineageEmitterFromFactory } from "df/cli/api/lineage/emitter_factory";
import { prettyJsonStringify } from "df/cli/api/utils";
import { buildDryRunValidationQueries } from "df/cli/api/utils/dry_run_validation";
import {
  dryRunOption,
  executionGraphOption,
  executionTimeoutOption,
  IRunArgs,
  runOptions,
  runTestsOption,
} from "df/cli/commands/run_options";
import { assertProjectDirExists, projectDirOption } from "df/cli/common_options";
import {
  Logger,
  print,
  printCompiledGraphErrors,
  printError,
  printExecutedAction,
  printExecutionGraph,
  printTestResult,
  printWarning,
} from "df/cli/console";
import { ProjectConfigOptions } from "df/cli/project_config_options";
import { setSignalHandler } from "df/cli/signals";
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
  readCredentials: dataform.IBigQuery | undefined,
  location?: string,
): LineageEmitter | undefined {
  return createLineageEmitterFromFactory({
    cliEmitLineage: argv.emitLineage,
    workflowLineageEnabled: executionGraph.projectConfig?.lineageEnabled ?? undefined,
    dryRun: !!argv.dryRun,
    projectDir: argv.projectDir || process.cwd(),
    readCredentials,
    location,
  });
}

export const runCommand: ICommand<IRunArgs> = {
  format: `run [${projectDirOption.name}]`,
  description: "Run the dataform project.",
  positionalOptions: [projectDirOption],
  check: [assertProjectDirExists],
  options: runOptions,
  processFn: async (argv) => {
    const logger = new Logger(!argv.json);

    if (argv.executionGraph && argv.dryRun) {
      printWarning(
        `Note: --${dryRunOption.name} is ignored because --${executionGraphOption.name} is set`,
      );
    }

    if (argv.executionGraph && argv.runTests) {
      printWarning(
        `Note: --${runTestsOption.name} is ignored because --${executionGraphOption.name} is set`,
      );
    }

    if (argv.timeout != null && argv.executionTimeout == null) {
      printWarning(
        "Note: --timeout only bounds project compilation. " +
          "For a whole-run wall-clock deadline, use --execution-timeout.\n",
      );
    }
    logger.log("Compiling...\n");
    // dataformCoreVersion projects are compiled in a temporary directory with the pinned
    // @dataform/core, and JiT compilation must use the same directory. It is removed when the
    // process exits (see createTemporaryDirectory and handleTerminationSignals).
    const { compiledGraph, environment } = await compileForRun({
      projectDir: argv.projectDir,
      projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
      timeoutMillis: argv.timeout || undefined,
    });
    if (compiledGraphHasErrors(compiledGraph)) {
      printCompiledGraphErrors(compiledGraph.graphErrors);
      return 1;
    }
    logger.success("Compiled successfully.\n");
    const readCredentials = credentials.read(actuallyResolve(argv.projectDir, argv.credentials));
    const defaultLocation = compiledGraph.projectConfig?.defaultLocation ?? undefined;
    if (
      defaultLocation &&
      readCredentials.location &&
      defaultLocation.toLowerCase() !== readCredentials.location.toLowerCase()
    ) {
      printWarning(
        `Warning: defaultLocation "${defaultLocation}" in the project config differs from ` +
          `location "${readCredentials.location}" in the credentials file. BigQuery jobs and ` +
          `new datasets will use "${defaultLocation}".\n`,
      );
    }

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
        jitTimeoutMillis: argv.jitTimeout || undefined,
      },
      dbadapter,
    );

    // BigQuery rejects jobs with more than 64 labels, so check before anything runs.
    const jobLabelLimitError = checkJobLabelLimit(
      executionGraph.actions,
      argv.runTests ? compiledGraph.tests : [],
      argv.jobLabels,
    );
    if (jobLabelLimitError) {
      printError(jobLabelLimitError);
      return 1;
    }

    if (argv.executionGraph) {
      if (!executionGraph.actions.some((action) => !!action.jitCode)) {
        printWarning(`Note: with --${executionGraphOption.name}, JiT actions are not compiled`);
      }
      printExecutionGraph(executionGraph, argv.json);
      return 0;
    }

    if (argv.runTests) {
      logger.log(`Running ${compiledGraph.tests.length} unit tests...\n`);
      const testResults = await test(dbadapter, compiledGraph.tests, argv.jobLabels);
      if (!argv.json) {
        testResults.forEach((testResult) => printTestResult(testResult));
      } else {
        testResults
          .filter((testResult) => !testResult.successful)
          .forEach((testResult) => {
            printError(`${testResult.name}: failed`);
            testResult.messages?.forEach((message) => printError(message, 1));
          });
      }
      if (testResults.some((testResult) => !testResult.successful)) {
        printError("\nUnit tests did not pass; aborting run.");
        return 1;
      }
      logger.success("Unit tests completed successfully.\n");
    }

    let bigqueryOptions: {} = {
      actionRetryLimit: argv.actionRetryLimit,
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
    executionGraph.actions.forEach((action) => {
      actionsByName.set(targetAsReadableString(action.target), action);
    });

    if (actionsByName.size === 0) {
      if (argv.json) {
        print(
          prettyJsonStringify({
            status: dataform.RunResult.ExecutionStatus.SUCCESSFUL,
            actions: [],
          }),
        );
      } else {
        logger.log("No actions to run.\n");
      }
      return 0;
    }

    if (argv.dryRun) {
      logger.log("Dry running (no changes to the warehouse will be applied)...");
    } else {
      logger.log("Running...\n");
    }

    const lineageEmitter = createLineageEmitter(
      argv,
      executionGraph,
      readCredentials,
      defaultLocation,
    );

    const runner = run(dbadapter, executionGraph, {
      projectDir: environment.projectDir,
      expectedCoreVersion: compiledGraph.dataformCoreVersion || undefined,
      bigquery: bigqueryOptions,
      lineageEmitter,
      dryRunValidationQueries: argv.dryRun
        ? buildDryRunValidationQueries(compiledGraph)
        : undefined,
    });
    // The first Ctrl+C cancels the run gracefully; a second one exits immediately.
    setSignalHandler("SIGINT", () => runner.cancel());

    const alreadyPrintedActions = new Set<string>();

    const printExecutedGraph = (executedGraph: dataform.IRunResult) => {
      executedGraph.actions
        .filter(
          (actionResult) => actionResult.status !== dataform.ActionResult.ExecutionStatus.RUNNING,
        )
        .filter(
          (executedAction) =>
            !alreadyPrintedActions.has(targetAsReadableString(executedAction.target)),
        )
        .forEach((executedAction) => {
          printExecutedAction(
            executedAction,
            actionsByName.get(targetAsReadableString(executedAction.target)),
            argv.dryRun,
          );
          alreadyPrintedActions.add(targetAsReadableString(executedAction.target));
        });
    };

    if (!argv.json) {
      runner.onChange(printExecutedGraph);
    }
    const runResult = await runner.result();
    if (lineageEmitter) {
      await lineageEmitter.drain(LINEAGE_DRAIN_TIMEOUT_MS);
    }
    if (argv.json) {
      print(prettyJsonStringify(runResult));
    } else {
      printExecutedGraph(runResult);
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
  },
};
