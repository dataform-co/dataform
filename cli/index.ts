import * as chokidar from "chokidar";
import * as fs from "fs";
import parseDuration from "parse-duration";
import * as path from "path";
import yargs from "yargs";

import { build, compile, credentials, prune, run, test } from "df/cli/api";
import { CREDENTIALS_FILENAME } from "df/cli/api/commands/credentials";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { createLineageEmitter as createLineageEmitterFromFactory } from "df/cli/api/lineage/emitter_factory";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  formatCommand,
  helpCommand,
  initCommand,
  initCredsCommand,
  installCommand
} from "df/cli/commands";
import {
  actionsOption,
  projectDirMustExistOption,
  projectDirOption,
  splitCommas
} from "df/cli/common_options";
import {
  compiledGraphOutputType,
  Logger,
  print,
  printCompiledGraph,
  printCompiledGraphErrors,
  printError,
  printExecutedAction,
  printExecutionGraph,
  printSuccess,
  printTestResult,
  printWarning
} from "df/cli/console";
import { ProjectConfigOptions } from "df/cli/project_config_options";
import {
  actuallyResolve,
  compiledGraphHasErrors,
} from "df/cli/util";
import { createYargsCli, INamedOption } from "df/cli/yargswrapper";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";

const RECOMPILE_DELAY = 500;

// Maximum time to wait for outstanding lineage emissions to complete before
// `dataform run` returns. Lineage emission is fail-open — if we don't drain
// within this window, in-flight requests are abandoned and the run status is
// unaffected.
const LINEAGE_DRAIN_TIMEOUT_MS = 15_000;

process.on("unhandledRejection", async (reason: any) => {
  printError(`Unhandled promise rejection: ${reason?.stack || reason}`);
});

// TODO: Since yargs launched an actually well typed API in version 12, let's use it as this file is currently not type checked.


const fullRefreshOption: INamedOption<yargs.Options> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

// It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
const requiresSelection = (
  name: string,
  actions: INamedOption<yargs.Options>,
  tags: INamedOption<yargs.Options>
): INamedOption<yargs.Options>["check"] => (argv: yargs.Arguments) => {
  if (argv[name] && !(argv[actions.name] || argv[tags.name])) {
    throw new Error(
      `The --${name} flag should only be supplied along with --${actions.name} or --${tags.name}.`
    );
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

// `compile` reuses the same prune() filtering as run/build, but these flags only
// filter the *printed output* -- the whole project still compiles. The `output-`
// prefix makes that distinction explicit.
const outputActionsOption: INamedOption<yargs.Options> = {
  name: "output-actions",
  option: {
    // No wildcard support: prune()'s matchPatterns() does exact matching on the
    // action name or its fully-qualified `database.schema.name`.
    describe: "A list of action names to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputTagsOption: INamedOption<yargs.Options> = {
  name: "output-tags",
  option: {
    describe: "A list of tags to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputIncludeDepsOption: INamedOption<yargs.Options> = {
  name: "output-include-deps",
  option: {
    describe: "If set, dependencies of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-deps", outputActionsOption, outputTagsOption)
};

const outputIncludeDependentsOption: INamedOption<yargs.Options> = {
  name: "output-include-dependents",
  option: {
    describe:
      "If set, dependents (downstream) of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-dependents", outputActionsOption, outputTagsOption)
};

const credentialsOption: INamedOption<yargs.Options> = {
  name: "credentials",
  option: {
    describe: "The location of the credentials JSON file to use.",
    default: CREDENTIALS_FILENAME
  },
  check: (argv: yargs.Arguments<any>) =>
    getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
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

const jsonOutputOption: INamedOption<yargs.Options> = {
  name: "json",
  option: {
    describe: "Outputs a JSON representation of the compiled project or test results.",
    type: "boolean",
    default: false
  }
};

const dotOutputOption: INamedOption<yargs.Options> = {
  name: "dot",
  option: {
    describe: "Outputs a dot representation of the compiled project.",
    type: "boolean",
    default: false,
  },
    check: (argv: yargs.Arguments<any>) => {
      if (argv.json && argv.dot) {
        throw new Error("Arguments --json and --dot are mutually exclusive.");
      }
    }
  
};

const timeoutOption: INamedOption<yargs.Options> = {
  name: "timeout",
  option: {
    describe: "Duration to allow project compilation to complete. Examples: '1s', '10m', etc.",
    type: "string",
    default: null,
    coerce: (rawTimeoutString: string | null) =>
      rawTimeoutString ? parseDuration(rawTimeoutString) : null
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
    coerce: (rawTimeoutString: string | null) =>
      rawTimeoutString ? parseDuration(rawTimeoutString) : null
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
    coerce: (rawTimeoutString: string | null) =>
      rawTimeoutString ? parseDuration(rawTimeoutString) : null
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

const quietCompileOption: INamedOption<yargs.Options> = {
  name: "quiet",
  option: {
    describe: "Less verbose compilation output. Example usage: 'dataform compile --quiet'",
    type: "boolean",
    default: false
  }
};

const watchOptionName = "watch";

const verboseOptionName = "verbose";
const dryRunOptionName = "dry-run";
const runTestsOptionName = "run-tests";

const actionRetryLimitName = "action-retry-limit";

function getCredentialsPath(projectDir: string, credentialsPath: string) {
  return actuallyResolve(projectDir, credentialsPath);
}

export function runCli() {
  const builtYargs = createYargsCli({
    commands: [
      helpCommand,
      initCommand,
      installCommand,
      initCredsCommand,
      {
        format: `compile [${projectDirMustExistOption.name}]`,
        description:
          "Compile the dataform project. Produces JSON output describing the non-executable graph.",
        positionalOptions: [projectDirMustExistOption],
        options: [
          {
            name: watchOptionName,
            option: {
              describe: "Whether to watch the changes in the project directory.",
              type: "boolean",
              default: false
            }
          },
          jsonOutputOption,
          dotOutputOption,
          timeoutOption,
          quietCompileOption,
          outputActionsOption,
          outputTagsOption,
          outputIncludeDepsOption,
          outputIncludeDependentsOption,
          {
            name: verboseOptionName,
            option: {
              describe: "Enable verbose compilation output. Example usage: 'dataform compile --verbose'",
              type: "boolean",
              default: false
            },
            check: (argv: yargs.Arguments) => {
              if (argv.quiet && argv.verbose) {
                throw new Error("Arguments --verbose and --quiet are mutually exclusive.");
              }
            }
          },
          ...ProjectConfigOptions.allYargsOptions
        ],
        processFn: async argv => {
          const projectDir = argv[projectDirMustExistOption.name];
          const logger = new Logger(!argv[jsonOutputOption.name]);

          async function compileAndPrint() {

            let outputType = compiledGraphOutputType.Summary;
            if (argv[jsonOutputOption.name]) {
              outputType = compiledGraphOutputType.Json;
            } else if (argv[dotOutputOption.name]) {
              outputType = compiledGraphOutputType.Dot;
            }

            if (outputType === compiledGraphOutputType.Summary) {
              logger.log("Compiling...\n");
            }
            const compiledGraph = await compile({
              projectDir,
              projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
              timeoutMillis: argv[timeoutOption.name] || undefined,
              verbose: argv[verboseOptionName] || false
            });

            // The whole project must compile (ref() resolution needs every action
            // registered), but the printed output can be filtered to the selected
            // action(s) -- mirroring how `run`/`build` prune the graph. We only prune
            // a clean graph; if compilation produced errors we print the full graph
            // plus the errors, keeping graph-level errors as-is.
            const hasSelector =
              argv[outputActionsOption.name]?.length > 0 || argv[outputTagsOption.name]?.length > 0;
            const outputGraph =
              hasSelector && !compiledGraphHasErrors(compiledGraph)
                ? prune(compiledGraph, {
                    actions: argv[outputActionsOption.name],
                    tags: argv[outputTagsOption.name],
                    includeDependencies: argv[outputIncludeDepsOption.name],
                    includeDependents: argv[outputIncludeDependentsOption.name]
                  })
                : compiledGraph;
            printCompiledGraph(outputGraph, outputType, argv[quietCompileOption.name]);
            if (compiledGraphHasErrors(compiledGraph)) {
              print("");
              printCompiledGraphErrors(compiledGraph.graphErrors, argv[quietCompileOption.name]);
              return true;
            }
            return false;
          }

          const graphHasErrors = await compileAndPrint();

          if (!argv[watchOptionName]) {
            return graphHasErrors ? 1 : 0;
          }

          let watching = true;

          let timeoutID: NodeJS.Timer = null;
          let isCompiling = false;

          // Initialize watcher.
          const watcher = chokidar.watch(projectDir, {
            ignored: /node_modules/,
            persistent: true,
            ignoreInitial: true,
            awaitWriteFinish: {
              stabilityThreshold: 1000,
              pollInterval: 200
            }
          });

          const printReady = () => {
            print("\nWatching for changes...\n");
          };
          // Add event listeners.
          watcher
            .on("ready", printReady)
            .on("error", error => {
              // This error is caught not if there is a compilation error, but
              // if the watcher fails; this indicates an failure on our side.
              printError(`Error: ${error}`);
              process.exit(1);
            })
            .on("all", () => {
              if (timeoutID || isCompiling) {
                // don't recompile many times if we changed a lot of files
                clearTimeout(timeoutID);
              }

              timeoutID = setTimeout(async () => {
                clearTimeout(timeoutID);

                if (!isCompiling) {
                  isCompiling = true;
                  await compileAndPrint();
                  printReady();
                  isCompiling = false;
                }
              }, RECOMPILE_DELAY);
            });
          process.on("SIGINT", async () => {
            await watcher.close();
            watching = false;
            process.exit(1);
          });
          while (watching) {
            await new Promise((resolve, reject) => setTimeout(() => resolve(), 100));
          }
        }
      },
      {
        format: `test [${projectDirMustExistOption.name}]`,
        description: "Run the dataform project's unit tests.",
        positionalOptions: [projectDirMustExistOption],
        options: [credentialsOption, timeoutOption, jsonOutputOption, ...ProjectConfigOptions.allYargsOptions],
        processFn: async argv => {
          if (!argv[jsonOutputOption.name]) {
            print("Compiling...\n");
          }          
          const compiledGraph = await compile({
            projectDir: argv[projectDirMustExistOption.name],
            projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
            timeoutMillis: argv[timeoutOption.name] || undefined
          });
          if (compiledGraphHasErrors(compiledGraph)) {
            printCompiledGraphErrors(compiledGraph.graphErrors, argv[quietCompileOption.name]);
            return 1;
          }
          if (!argv[jsonOutputOption.name]) {
            printSuccess("Compiled successfully.\n");
          }   
          const readCredentials = credentials.read(
            getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
          );

          if (!compiledGraph.tests.length) {
            printError("No unit tests found.");
            return 1;
          }

          if (!argv[jsonOutputOption.name]) {
            print(`Running ${compiledGraph.tests.length} unit tests...\n`);
          }
          const dbadapter = new BigQueryDbAdapter(readCredentials);
          const testResults = await test(dbadapter, compiledGraph.tests);
          if (!argv[jsonOutputOption.name]) {
            testResults.forEach(testResult => printTestResult(testResult));
          } else {
            // Print all results as JSON if the option is set.
            print(prettyJsonStringify(testResults));
          }
          return testResults.every(testResult => testResult.successful) ? 0 : 1;
        }
      },
      {
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
            return;
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
            getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
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
      },
      formatCommand
    ]
  })
    .scriptName("dataform")
    .strict()
    .wrap(null)
    .recommendCommands()
    .fail(async (msg: string, err: any) => {
      if (!!err && err.name === "VMError" && err.message.includes("Cannot find module")) {
        printError("Could not find NPM dependencies. Have you run 'dataform install'?");
      } else {
        const message = err?.message ? err.message.split("\n")[0] : msg;
        printError(`Dataform encountered an error: ${message}`);
        if (err?.stack) {
          printError(err.stack);
        }
      }
      process.exit(1);
    }).argv;

  // If no command is specified, show top-level help string.
  if (!builtYargs._[0]) {
    yargs.showHelp();
  }
}

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
