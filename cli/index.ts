#!/usr/bin/env node
import { build, compile, credentials, init, run, table, test } from "@dataform/api";
import { prettyJsonStringify } from "@dataform/api/utils";
import {
  print,
  printCompiledGraph,
  printCompiledGraphErrors,
  printError,
  printExecutedAction,
  printExecutionGraph,
  printGetTableResult,
  printInitCredsResult,
  printInitResult,
  printListTablesResult,
  printSuccess,
  printTestResult
} from "@dataform/cli/console";
import {
  getBigQueryCredentials,
  getPostgresCredentials,
  getRedshiftCredentials,
  getSQLDataWarehouseCredentials,
  getSnowflakeCredentials,
} from "@dataform/cli/credentials";
import { actuallyResolve, assertPathExists, compiledGraphHasErrors } from "@dataform/cli/util";
import { createYargsCli, INamedOption } from "@dataform/cli/yargswrapper";
import { supportsCancel, WarehouseType } from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import * as chokidar from "chokidar";
import * as fs from "fs";
import * as path from "path";
import * as yargs from "yargs";

const RECOMPILE_DELAY = 500;

const projectDirOption: INamedOption<yargs.PositionalOptions> = {
  name: "project-dir",
  option: {
    describe: "The Dataform project directory.",
    default: ".",
    coerce: actuallyResolve
  }
};

const projectDirMustExistOption = {
  ...projectDirOption,
  check: (argv: yargs.Arguments) => {
    assertPathExists(argv["project-dir"]);
    try {
      assertPathExists(path.resolve(argv["project-dir"], "dataform.json"));
    } catch (e) {
      throw new Error(
        `${
          argv["project-dir"]
        } does not appear to be a dataform directory (missing dataform.json file).`
      );
    }
  }
};

const fullRefreshOption: INamedOption<yargs.Options> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

const actionsOption: INamedOption<yargs.Options> = {
  name: "actions",
  option: {
    describe: "A list of action names or patterns to run. Can include '*' wildcards.",
    type: "array"
  }
};

const includeDepsOption: INamedOption<yargs.Options> = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected actions will also be run.",
    type: "boolean"
  },
  // It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
  check: (argv: yargs.Arguments) => {
    if (argv.include_deps && !argv.actions) {
      throw new Error("The --include_deps flag should only be supplied along with --actions.");
    }
  }
};

const schemaSuffixOverrideOption: INamedOption<yargs.Options> = {
  name: "schema-suffix",
  option: {
    describe: "A suffix to be appended to output schema names."
  },
  check: (argv: yargs.Arguments) => {
    if (argv.schemaSuffix && !/^[a-zA-Z_0-9]+$/.test(argv.schemaSuffix)) {
      throw new Error(
        "--schema-suffix should contain only alphanumeric characters and/or underscores."
      );
    }
  }
};

const credentialsOption: INamedOption<yargs.Options> = {
  name: "credentials",
  option: {
    describe: "The location of the credentials JSON file to use.",
    default: credentials.CREDENTIALS_FILENAME,
    coerce: actuallyResolve
  },
  check: (argv: yargs.Arguments) => assertPathExists(argv.credentials)
};

const warehouseOption: INamedOption<yargs.PositionalOptions> = {
  name: "warehouse",
  option: {
    describe: "The project's data warehouse type.",
    choices: Object.keys(WarehouseType).map(warehouseType => WarehouseType[warehouseType])
  }
};

const verboseOutputOption: INamedOption<yargs.Options> = {
  name: "verbose",
  option: {
    describe:
      "If true, the full contents of command output will be output (containing fully compiled SQL, etc).",
    type: "boolean",
    default: false
  }
};

const builtYargs = createYargsCli({
  commands: [
    {
      format: "init <warehouse> [project-dir]",
      description: "Create a new dataform project.",
      positionalOptions: [warehouseOption, projectDirOption],
      options: [
        {
          name: "gcloud-project-id",
          option: {
            describe: "The Google Cloud Project ID to use when accessing bigquery."
          },
          check: (argv: yargs.Arguments) => {
            if (argv["gcloud-project-id"] && argv.warehouse !== "bigquery") {
              throw new Error("The --gcloud-project-id flag is only used for BigQuery projects.");
            }
            if (!argv["gcloud-project-id"] && argv.warehouse === "bigquery") {
              throw new Error(
                "The --gcloud-project-id flag is required for BigQuery projects. Please run 'dataform help init' for more information."
              );
            }
          }
        },
        {
          name: "skip-install",
          option: {
            describe: "Whether to skip installing NPM packages.",
            default: false
          }
        },
        {
          name: "include-schedules",
          option: {
            describe: "Whether to initialize a schedules.json file.",
            default: false
          }
        }
      ],
      processFn: async argv => {
        print("Writing project files...\n");
        printInitResult(
          await init(
            argv["project-dir"],
            {
              warehouse: argv.warehouse,
              gcloudProjectId: argv["gcloud-project-id"]
            },
            {
              skipInstall: argv["skip-install"],
              includeSchedules: argv["include-schedules"]
            }
          )
        );
      }
    },
    {
      format: "init-creds <warehouse> [project-dir]",
      description: `Creates a ${
        credentials.CREDENTIALS_FILENAME
      } file for dataform to use when accessing your warehouse.`,
      positionalOptions: [warehouseOption, projectDirMustExistOption],
      options: [
        {
          name: "test-connection",
          option: {
            describe: "If true, a test query will be run using your final credentials.",
            type: "boolean",
            default: true
          }
        }
      ],
      processFn: async argv => {
        const credentialsFn = () => {
          switch (argv.warehouse) {
            case "bigquery": {
              return getBigQueryCredentials();
            }
            case "postgres": {
              return getPostgresCredentials();
            }
            case "redshift": {
              return getRedshiftCredentials();
            }
            case "sqldatawarehouse":{
              return getSQLDataWarehouseCredentials();
            }
            case "snowflake": {
              return getSnowflakeCredentials();
            }
            default: {
              throw new Error(`Unrecognized warehouse type ${argv.warehouse}`);
            }
          }
        };
        const finalCredentials = credentialsFn();
        if (argv["test-connection"]) {
          print("\nRunning connection test...");
          const testResult = await credentials.test(finalCredentials, argv.warehouse);
          switch (testResult.status) {
            case credentials.TestResultStatus.SUCCESSFUL: {
              printSuccess("\nWarehouse test query completed successfully.\n");
              break;
            }
            case credentials.TestResultStatus.TIMED_OUT: {
              throw new Error("Warehouse test connection timed out.");
            }
            case credentials.TestResultStatus.OTHER_ERROR: {
              throw new Error(
                `Warehouse test query failed: ${testResult.error.stack || testResult.error.message}`
              );
            }
          }
        } else {
          print("\nWarehouse test query was not run.\n");
        }
        const filePath = path.resolve(argv["project-dir"], credentials.CREDENTIALS_FILENAME);
        fs.writeFileSync(filePath, prettyJsonStringify(finalCredentials));
        printInitCredsResult(filePath);
      }
    },
    {
      format: "compile [project-dir]",
      description:
        "Compile the dataform project. Produces JSON output describing the non-executable graph.",
      positionalOptions: [projectDirMustExistOption],
      options: [
        {
          name: "watch",
          option: {
            describe: "Whether to watch the changes in the project directory.",
            type: "boolean",
            default: false
          }
        },
        schemaSuffixOverrideOption,
        verboseOutputOption
      ],
      processFn: async argv => {
        const projectDir = argv["project-dir"];
        const schemaSuffixOverride = argv["schema-suffix"];

        const compileAndPrint = async () => {
          print("Compiling...\n");
          const compiledGraph = await compile({
            projectDir,
            schemaSuffixOverride
          });
          printCompiledGraph(compiledGraph, argv.verbose);
          if (compiledGraphHasErrors(compiledGraph)) {
            print("");
            printCompiledGraphErrors(compiledGraph.graphErrors);
          }
        };
        await compileAndPrint();

        if (argv.watch) {
          let timeoutID = null;
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

          let watching = true;

          const printReady = () => {
            print("\nWatching for changes...\n");
          };
          // Add event listeners.
          watcher
            .on("ready", printReady)
            .on("error", error => printError(`Error: ${error}`))
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
          process.on("SIGINT", () => {
            watcher.close();
            watching = false;
          });
          while (watching) {
            await new Promise((resolve, reject) => setTimeout(() => resolve(), 100));
          }
        }
      }
    },
    {
      format: "test [project-dir]",
      description: "Run the dataform project's unit tests on the configured data warehouse.",
      positionalOptions: [projectDirMustExistOption],
      options: [credentialsOption],
      processFn: async argv => {
        print("Compiling...\n");
        const compiledGraph = await compile({
          projectDir: argv["project-dir"],
          schemaSuffixOverride: argv["schema-suffix"]
        });
        if (compiledGraphHasErrors(compiledGraph)) {
          printCompiledGraphErrors(compiledGraph.graphErrors);
          return;
        }
        printSuccess("Compiled successfully.\n");
        const readCredentials = credentials.read(
          compiledGraph.projectConfig.warehouse,
          argv.credentials
        );

        if (!compiledGraph.tests.length) {
          printError("No unit tests found.");
          return;
        }

        print(`Running ${compiledGraph.tests.length} unit tests...\n`);
        const testResults = await test(
          readCredentials,
          compiledGraph.projectConfig.warehouse,
          compiledGraph.tests
        );
        testResults.forEach(testResult => printTestResult(testResult));
      }
    },
    {
      format: "run [project-dir]",
      description: "Run the dataform project's scripts on the configured data warehouse.",
      positionalOptions: [projectDirMustExistOption],
      options: [
        {
          name: "dry-run",
          option: {
            describe:
              "If set, built SQL is not run against the data warehouse and instead is printed to the console.",
            type: "boolean"
          }
        },
        {
          name: "run-tests",
          option: {
            describe:
              "If set, the project's unit tests are required to pass before running the project.",
            type: "boolean"
          }
        },
        fullRefreshOption,
        actionsOption,
        includeDepsOption,
        schemaSuffixOverrideOption,
        credentialsOption,
        verboseOutputOption
      ],
      processFn: async argv => {
        print("Compiling...\n");
        const compiledGraph = await compile({
          projectDir: argv["project-dir"],
          schemaSuffixOverride: argv["schema-suffix"]
        });
        if (compiledGraphHasErrors(compiledGraph)) {
          printCompiledGraphErrors(compiledGraph.graphErrors);
          return;
        }
        printSuccess("Compiled successfully.\n");
        const readCredentials = credentials.read(
          compiledGraph.projectConfig.warehouse,
          argv.credentials
        );
        const executionGraph = await build(
          compiledGraph,
          {
            fullRefresh: argv["full-refresh"],
            actions: argv.actions,
            includeDependencies: argv["include-deps"]
          },
          readCredentials
        );

        if (argv["dry-run"]) {
          print(
            "Dry run (--dry-run) mode is turned on; not running the following actions against your warehouse:\n"
          );
          printExecutionGraph(executionGraph, argv.verbose);
          return;
        }

        if (argv["run-tests"]) {
          print(`Running ${compiledGraph.tests.length} unit tests...\n`);
          const testResults = await test(
            readCredentials,
            compiledGraph.projectConfig.warehouse,
            compiledGraph.tests
          );
          testResults.forEach(testResult => printTestResult(testResult));
          if (testResults.some(testResult => !testResult.successful)) {
            printError("\nUnit tests did not pass; aborting run.");
            return;
          }
          printSuccess("Unit tests completed successfully.\n");
        }

        print("Running...\n");
        const runner = run(executionGraph, readCredentials);
        process.on("SIGINT", () => {
          if (!supportsCancel(WarehouseType[compiledGraph.projectConfig.warehouse])) {
            process.exit();
          }
          runner.cancel();
        });

        const actionsByName = new Map<string, dataform.IExecutionAction>();
        executionGraph.actions.forEach(action => {
          actionsByName.set(action.name, action);
        });
        const alreadyPrintedActions = new Set<string>();

        const printExecutedGraph = (executedGraph: dataform.IExecutedGraph) => {
          executedGraph.actions
            .filter(executedAction => !alreadyPrintedActions.has(executedAction.name))
            .forEach(executedAction => {
              printExecutedAction(
                executedAction,
                actionsByName.get(executedAction.name),
                argv.verbose
              );
              alreadyPrintedActions.add(executedAction.name);
            });
        };

        runner.onChange(printExecutedGraph);
        printExecutedGraph(await runner.resultPromise());
      }
    },
    {
      format: "listtables <warehouse>",
      description: "List tables on the configured data warehouse.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: async argv => {
        printListTablesResult(
          await table.list(credentials.read(argv.warehouse, argv.credentials), argv.warehouse)
        );
      }
    },
    {
      format: "gettablemetadata <warehouse> <schema> <table>",
      description: "Fetch metadata for a specified table.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: async argv => {
        printGetTableResult(
          await table.get(credentials.read(argv.warehouse, argv.credentials), argv.warehouse, {
            schema: argv.schema,
            name: argv.table
          })
        );
      }
    }
  ]
})
  .scriptName("dataform")
  .strict()
  .wrap(null)
  .recommendCommands()
  .fail((msg, err) => {
    const message = err ? err.message.split("\n")[0] : msg;
    printError(`Dataform encountered an error: ${message}`);
    process.exit(1);
  }).argv;

// If no command is specified, show top-level help string.
if (!builtYargs._[0]) {
  yargs.showHelp();
}
