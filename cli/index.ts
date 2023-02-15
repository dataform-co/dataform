import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import yargs from "yargs";

import * as chokidar from "chokidar";
import { build, compile, credentials, init, install, run, table, test } from "df/api";
import { CREDENTIALS_FILENAME } from "df/api/commands/credentials";
import * as dbadapters from "df/api/dbadapters";
import { prettyJsonStringify } from "df/api/utils";
import { trackError } from "df/cli/analytics";
import {
  print,
  printCompiledGraph,
  printCompiledGraphErrors,
  printError,
  printExecutedAction,
  printExecutionGraph,
  printFormatFilesResult,
  printGetTableResult,
  printInitCredsResult,
  printInitResult,
  printListTablesResult,
  printSuccess,
  printTestResult
} from "df/cli/console";
import {
  getBigQueryCredentials,
  getPostgresCredentials,
  getRedshiftCredentials,
  getSnowflakeCredentials,
  getSQLDataWarehouseCredentials
} from "df/cli/credentials";
import { actuallyResolve, assertPathExists, compiledGraphHasErrors } from "df/cli/util";
import { createYargsCli, INamedOption } from "df/cli/yargswrapper";
import { supportsCancel, WarehouseType } from "df/core/adapters";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { formatFile } from "df/sqlx/format";
import parseDuration from "parse-duration";

const RECOMPILE_DELAY = 500;

process.on("unhandledRejection", async (reason: any) => {
  printError(`Unhandled promise rejection: ${reason?.stack || reason}`);
  await trackError();
});

// TODO: Since yargs launched an actually well typed API in version 12, let's use it as this file is currently not type checked.

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
  check: (argv: yargs.Arguments<any>) => {
    assertPathExists(argv[projectDirOption.name]);
    try {
      assertPathExists(path.resolve(argv[projectDirOption.name], "dataform.json"));
    } catch (e) {
      throw new Error(
        `${
          argv[projectDirOption.name]
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

const tagsOption: INamedOption<yargs.Options> = {
  name: "tags",
  option: {
    describe: "A list of tags to filter the actions to run.",
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
    if (argv[includeDepsOption.name] && !(argv[actionsOption.name] || argv[tagsOption.name])) {
      throw new Error(
        `The --${includeDepsOption.name} flag should only be supplied along with --${actionsOption.name} or --${tagsOption.name}.`
      );
    }
  }
};

const includeDependentsOption: INamedOption<yargs.Options> = {
  name: "include-dependents",
  option: {
    describe: "If set, dependents (downstream) for selected actions will also be run.",
    type: "boolean"
  },
  // It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
  check: (argv: yargs.Arguments) => {
    if (
      argv[includeDependentsOption.name] &&
      !(argv[actionsOption.name] || argv[tagsOption.name])
    ) {
      throw new Error(
        `The --${includeDependentsOption.name} flag should only be supplied along with --${actionsOption.name} or --${tagsOption.name}.`
      );
    }
  }
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

const warehouseOption: INamedOption<yargs.PositionalOptions> = {
  name: "warehouse",
  option: {
    describe: "The project's data warehouse type.",
    choices: Object.values(WarehouseType)
  }
};

const jsonOutputOption: INamedOption<yargs.Options> = {
  name: "json",
  option: {
    describe: "Outputs a JSON representation of the compiled project.",
    type: "boolean",
    default: false
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

const defaultDatabaseOption: INamedOption<yargs.Options> = {
  name: "default-database",
  option: {
    describe: "The default database to use. For BigQuery, this is a Google Cloud Project ID.",
    type: "string"
  },
  check: (argv: yargs.Arguments<any>) => {
    if (!argv[warehouseOption.name]) {
      return;
    }
    if (
      argv[defaultDatabaseOption.name] &&
      !["bigquery", "snowflake"].includes(argv[warehouseOption.name])
    ) {
      throw new Error(
        `The --${defaultDatabaseOption.name} flag is only used for BigQuery and Snowflake projects.`
      );
    }
    if (!argv[defaultDatabaseOption.name] && argv[warehouseOption.name] === "bigquery") {
      throw new Error(
        `The --${defaultDatabaseOption.name} flag is required for BigQuery projects.`
      );
    }
  }
};

const defaultLocationOption: INamedOption<yargs.Options> = {
  name: "default-location",
  option: {
    describe:
      "The default BigQuery location to use. See https://cloud.google.com/bigquery/docs/locations for supported values."
  },
  check: (argv: yargs.Arguments<any>) => {
    if (!argv[warehouseOption.name]) {
      return;
    }
    if (argv[defaultLocationOption.name] && !["bigquery"].includes(argv[warehouseOption.name])) {
      throw new Error(`The --${defaultLocationOption.name} flag is only used for BigQuery.`);
    }
    if (!argv[defaultLocationOption.name] && argv[warehouseOption.name] === "bigquery") {
      throw new Error(
        `The --${defaultLocationOption.name} flag is required for BigQuery projects. Please run 'dataform help init' for more information.`
      );
    }
  }
};

const skipInstallOptionName = "skip-install";

const testConnectionOptionName = "test-connection";

const watchOptionName = "watch";

const dryRunOptionName = "dry-run";
const runTestsOptionName = "run-tests";

const schemaOptionName = "schema";
const tableOptionName = "table";

const getCredentialsPath = (projectDir: string, credentialsPath: string) =>
  actuallyResolve(credentialsPath || path.join(projectDir, CREDENTIALS_FILENAME));

export function runCli() {
  const builtYargs = createYargsCli({
    commands: [
      {
        // This dummy command is a hack with the only goal of displaying "help" as a command in the CLI
        // and we need it because of the limitations of yargs considering "help" as an option and not as a command.
        format: "help [command]",
        description: "Show help. If [command] is specified, the help is for the given command.",
        positionalOptions: [],
        options: [],
        processFn: async argv => {
          return 0;
        }
      },
      {
        format: `init <${warehouseOption.name}> [${projectDirOption.name}]`,
        description: "Create a new dataform project.",
        positionalOptions: [warehouseOption, projectDirOption],
        options: [
          defaultDatabaseOption,
          defaultLocationOption,
          {
            name: skipInstallOptionName,
            option: {
              describe: "Whether to skip installing NPM packages.",
              default: false
            }
          }
        ],
        processFn: async argv => {
          print("Writing project files...\n");
          const initResult = await init(
            argv[projectDirOption.name],
            {
              warehouse: argv[warehouseOption.name],
              defaultDatabase: argv[defaultDatabaseOption.name],
              defaultLocation: argv[defaultLocationOption.name],
              useRunCache: false
            },
            {
              skipInstall: argv[skipInstallOptionName]
            }
          );
          printInitResult(initResult);
          return 0;
        }
      },
      {
        format: `install [${projectDirMustExistOption.name}]`,
        description: "Install a project's NPM dependencies.",
        positionalOptions: [projectDirMustExistOption],
        options: [],
        processFn: async argv => {
          print("Installing NPM dependencies...\n");
          await install(argv[projectDirMustExistOption.name]);
          printSuccess("Project dependencies successfully installed.");
          return 0;
        }
      },
      {
        format: `init-creds <${warehouseOption.name}> [${projectDirMustExistOption.name}]`,
        description: `Create a ${credentials.CREDENTIALS_FILENAME} file for Dataform to use when accessing your warehouse.`,
        positionalOptions: [warehouseOption, projectDirMustExistOption],
        options: [
          {
            name: testConnectionOptionName,
            option: {
              describe: "If true, a test query will be run using your final credentials.",
              type: "boolean",
              default: true
            }
          }
        ],
        processFn: async argv => {
          const credentialsFn = () => {
            switch (argv[warehouseOption.name]) {
              case "bigquery": {
                return getBigQueryCredentials();
              }
              case "postgres": {
                return getPostgresCredentials();
              }
              case "redshift": {
                return getRedshiftCredentials();
              }
              case "sqldatawarehouse": {
                return getSQLDataWarehouseCredentials();
              }
              case "snowflake": {
                return getSnowflakeCredentials();
              }
              default: {
                throw new Error(`Unrecognized warehouse type ${argv[warehouseOption.name]}`);
              }
            }
          };
          const finalCredentials = credentialsFn();
          if (argv[testConnectionOptionName]) {
            print("\nRunning connection test...");
            const dbadapter = await dbadapters.create(finalCredentials, argv[warehouseOption.name]);
            try {
              const testResult = await credentials.test(dbadapter);
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
                    `Warehouse test query failed: ${testResult.error.stack ||
                      testResult.error.message}`
                  );
                }
              }
            } finally {
              await dbadapter.close();
            }
          } else {
            print("\nWarehouse test query was not run.\n");
          }
          const filePath = path.resolve(
            argv[projectDirMustExistOption.name],
            credentials.CREDENTIALS_FILENAME
          );
          fs.writeFileSync(filePath, prettyJsonStringify(finalCredentials));
          printInitCredsResult(filePath);
          return 0;
        }
      },
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
          timeoutOption,
          ...ProjectConfigOverride.allYargsOptions
        ],
        processFn: async argv => {
          const projectDir = argv[projectDirMustExistOption.name];

          const compileAndPrint = async () => {
            if (!argv[jsonOutputOption.name]) {
              print("Compiling...\n");
            }
            const compiledGraph = await compile({
              projectDir,
              projectConfigOverride: ProjectConfigOverride.construct(argv),
              timeoutMillis: argv[timeoutOption.name] || undefined
            });
            printCompiledGraph(compiledGraph, argv[jsonOutputOption.name]);
            if (compiledGraphHasErrors(compiledGraph)) {
              print("");
              printCompiledGraphErrors(compiledGraph.graphErrors);
              return true;
            }
            return false;
          };
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
          process.on("SIGINT", () => {
            watcher.close();
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
        description: "Run the dataform project's unit tests on the configured data warehouse.",
        positionalOptions: [projectDirMustExistOption],
        options: [credentialsOption, timeoutOption, ...ProjectConfigOverride.allYargsOptions],
        processFn: async argv => {
          print("Compiling...\n");
          const compiledGraph = await compile({
            projectDir: argv[projectDirMustExistOption.name],
            projectConfigOverride: ProjectConfigOverride.construct(argv),
            timeoutMillis: argv[timeoutOption.name] || undefined
          });
          if (compiledGraphHasErrors(compiledGraph)) {
            printCompiledGraphErrors(compiledGraph.graphErrors);
            return 1;
          }
          printSuccess("Compiled successfully.\n");
          const readCredentials = credentials.read(
            compiledGraph.projectConfig.warehouse,
            getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
          );

          if (!compiledGraph.tests.length) {
            printError("No unit tests found.");
            return 1;
          }

          print(`Running ${compiledGraph.tests.length} unit tests...\n`);
          const dbadapter = await dbadapters.create(
            readCredentials,
            compiledGraph.projectConfig.warehouse,
            { concurrencyLimit: compiledGraph.projectConfig.concurrentQueryLimit }
          );
          try {
            const testResults = await test(dbadapter, compiledGraph.tests);
            testResults.forEach(testResult => printTestResult(testResult));
            return testResults.every(testResult => testResult.successful) ? 0 : 1;
          } finally {
            await dbadapter.close();
          }
        }
      },
      {
        format: `run [${projectDirMustExistOption.name}]`,
        description: "Run the dataform project's scripts on the configured data warehouse.",
        positionalOptions: [projectDirMustExistOption],
        options: [
          {
            name: dryRunOptionName,
            option: {
              describe:
                "If set, built SQL is not run against the data warehouse and instead is printed to the console.",
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
          fullRefreshOption,
          actionsOption,
          tagsOption,
          includeDepsOption,
          includeDependentsOption,
          credentialsOption,
          jsonOutputOption,
          timeoutOption,
          ...ProjectConfigOverride.allYargsOptions
        ],
        processFn: async argv => {
          if (!argv[jsonOutputOption.name]) {
            print("Compiling...\n");
          }
          const compiledGraph = await compile({
            projectDir: argv[projectDirOption.name],
            projectConfigOverride: ProjectConfigOverride.construct(argv),
            timeoutMillis: argv[timeoutOption.name] || undefined
          });
          if (compiledGraphHasErrors(compiledGraph)) {
            printCompiledGraphErrors(compiledGraph.graphErrors);
            return 1;
          }
          if (!argv[jsonOutputOption.name]) {
            printSuccess("Compiled successfully.\n");
          }
          const readCredentials = credentials.read(
            compiledGraph.projectConfig.warehouse,
            getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
          );

          const dbadapter = await dbadapters.create(
            readCredentials,
            compiledGraph.projectConfig.warehouse,
            { concurrencyLimit: compiledGraph.projectConfig.concurrentQueryLimit }
          );
          try {
            const executionGraph = await build(
              compiledGraph,
              {
                fullRefresh: argv[fullRefreshOption.name],
                actions: argv[actionsOption.name],
                includeDependencies: argv[includeDepsOption.name],
                includeDependents: argv[includeDependentsOption.name],
                tags: argv[tagsOption.name]
              },
              dbadapter
            );

            if (argv[dryRunOptionName]) {
              if (!argv[jsonOutputOption.name]) {
                print(
                  `Dry run (--${dryRunOptionName}) mode is turned on; not running the following actions against your warehouse:\n`
                );
              }
              printExecutionGraph(executionGraph, argv[jsonOutputOption.name]);
              return;
            }

            if (argv[runTestsOptionName]) {
              print(`Running ${compiledGraph.tests.length} unit tests...\n`);
              const testResults = await test(dbadapter, compiledGraph.tests);
              testResults.forEach(testResult => printTestResult(testResult));
              if (testResults.some(testResult => !testResult.successful)) {
                printError("\nUnit tests did not pass; aborting run.");
                return 1;
              }
              printSuccess("Unit tests completed successfully.\n");
            }

            if (!argv[jsonOutputOption.name]) {
              print("Running...\n");
            }
            const runner = run(dbadapter, executionGraph);
            process.on("SIGINT", () => {
              if (
                !supportsCancel(
                  WarehouseType[compiledGraph.projectConfig.warehouse as keyof typeof WarehouseType]
                )
              ) {
                process.exit(1);
              }
              runner.cancel();
            });

            const actionsByName = new Map<string, dataform.IExecutionAction>();
            executionGraph.actions.forEach(action => {
              actionsByName.set(targetAsReadableString(action.target), action);
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
                    actionsByName.get(targetAsReadableString(executedAction.target))
                  );
                  alreadyPrintedActions.add(targetAsReadableString(executedAction.target));
                });
            };

            runner.onChange(printExecutedGraph);
            const runResult = await runner.result();
            printExecutedGraph(runResult);
            return runResult.status === dataform.RunResult.ExecutionStatus.SUCCESSFUL ? 0 : 1;
          } finally {
            await dbadapter.close();
          }
        }
      },
      {
        format: `format [${projectDirMustExistOption.name}]`,
        description: "Format the dataform project's files.",
        positionalOptions: [projectDirMustExistOption],
        options: [],
        processFn: async argv => {
          const filenames = glob.sync("{definitions,includes}/**/*.{js,sqlx}", {
            cwd: argv[projectDirMustExistOption.name]
          });
          const results = await Promise.all(
            filenames.map(async filename => {
              try {
                await formatFile(path.resolve(argv[projectDirMustExistOption.name], filename), {
                  overwriteFile: true
                });
                return {
                  filename
                };
              } catch (e) {
                return {
                  filename,
                  err: e
                };
              }
            })
          );
          printFormatFilesResult(results);
          return 0;
        }
      },
      {
        format: `listtables <${warehouseOption.name}>`,
        description: "List tables on the configured data warehouse.",
        positionalOptions: [warehouseOption],
        options: [credentialsOption],
        processFn: async argv => {
          const readCredentials = credentials.read(
            argv[warehouseOption.name],
            actuallyResolve(argv[credentialsOption.name])
          );
          const dbadapter = await dbadapters.create(readCredentials, argv[warehouseOption.name]);
          try {
            printListTablesResult(await table.list(dbadapter));
          } finally {
            await dbadapter.close();
          }
          return 0;
        }
      },
      {
        format: `gettablemetadata <${warehouseOption.name}> <${schemaOptionName}> <${tableOptionName}>`,
        description: "Fetch metadata for a specified table.",
        positionalOptions: [
          warehouseOption,
          {
            name: schemaOptionName,
            option: {
              describe: "The schema inside which the table exists.",
              type: "string"
            }
          },
          {
            name: tableOptionName,
            option: {
              describe: "The table's name.",
              type: "string"
            }
          }
        ],
        options: [credentialsOption],
        processFn: async argv => {
          const readCredentials = credentials.read(
            argv[warehouseOption.name],
            actuallyResolve(argv[credentialsOption.name])
          );
          const dbadapter = await dbadapters.create(readCredentials, argv[warehouseOption.name]);
          try {
            printGetTableResult(
              await table.get(dbadapter, {
                schema: argv[schemaOptionName],
                name: argv[tableOptionName]
              })
            );
          } finally {
            await dbadapter.close();
          }
          return 0;
        }
      }
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
        await trackError();
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

class ProjectConfigOverride {
  public static warehouseOverrideOption: INamedOption<yargs.Options> = {
    ...warehouseOption,
    option: {
      describe:
        "Override for the project's data warehouse type. If unset, the value from dataform.json is used.",
      type: "string"
    }
  };

  public static defaultDatabaseOverrideOption: INamedOption<yargs.Options> = {
    ...defaultDatabaseOption,
    option: {
      describe:
        "Override for the default database to use. For BigQuery, this is a " +
        "Google Cloud Project ID. If unset, the value from dataform.json is used.",
      type: "string"
    }
  };

  public static defaultSchemaOverrideOption: INamedOption<yargs.Options> = {
    name: "default-schema",
    option: {
      describe:
        "Override for the default schema name. If unset, the value from dataform.json is used."
    }
  };

  public static defaultLocationOverrideOption: INamedOption<yargs.Options> = {
    ...defaultLocationOption,
    option: {
      describe:
        "Override for the default BigQuery location to use. See " +
        "https://cloud.google.com/bigquery/docs/locations for supported values.If unset, the " +
        "value from dataform.json is used."
    }
  };

  public static assertionSchemaOverrideOption: INamedOption<yargs.Options> = {
    name: "assertion-schema",
    option: {
      describe: "Default assertion schema. If unset, the value from dataform.json is used."
    }
  };

  public static databaseSuffixOverrideOption: INamedOption<yargs.Options> = {
    name: "database-suffix",
    option: {
      describe: "Default assertion schema. If unset, the value from dataform.json is used."
    }
  };

  public static varsOverrideOption: INamedOption<yargs.Options> = {
    name: "vars",
    option: {
      describe:
        "Override for variables to inject via '--vars=someKey=someValue,a=b', referenced by " +
        "`dataform.projectConfig.vars.someValue`.  If unset, the value from dataform.json is used.",
      type: "string",
      default: null,
      coerce: (rawVarsString: string | null) => {
        const variables: { [key: string]: string } = {};
        rawVarsString?.split(",").forEach(keyValueStr => {
          const [key, value] = keyValueStr.split("=");
          variables[key] = value;
        });
        return variables;
      }
    }
  };

  public static schemaSuffixOverrideOption: INamedOption<yargs.Options> = {
    name: "schema-suffix",
    option: {
      describe:
        "A suffix to be appended to output schema names. If unset, the value from dataform.json is used."
    },
    check: (argv: yargs.Arguments<any>) => {
      if (
        argv[ProjectConfigOverride.schemaSuffixOverrideOption.name] &&
        !/^[a-zA-Z_0-9]+$/.test(argv[ProjectConfigOverride.schemaSuffixOverrideOption.name])
      ) {
        throw new Error(
          `--${ProjectConfigOverride.schemaSuffixOverrideOption.name} should contain only ` +
            `alphanumeric characters and/or underscores.`
        );
      }
    }
  };

  public static tablePrefixOverrideOption: INamedOption<yargs.Options> = {
    name: "table-prefix",
    option: {
      describe: "Adds a prefix for all table names. If unset, the value from dataform.json is used."
    }
  };

  public static allYargsOptions = [
    ProjectConfigOverride.warehouseOverrideOption,
    ProjectConfigOverride.defaultDatabaseOverrideOption,
    ProjectConfigOverride.defaultSchemaOverrideOption,
    ProjectConfigOverride.defaultLocationOverrideOption,
    ProjectConfigOverride.assertionSchemaOverrideOption,
    ProjectConfigOverride.varsOverrideOption,
    ProjectConfigOverride.databaseSuffixOverrideOption,
    ProjectConfigOverride.schemaSuffixOverrideOption,
    ProjectConfigOverride.tablePrefixOverrideOption
  ];

  public static construct(argv: yargs.Arguments<any>): dataform.IProjectConfig {
    const projectConfigOverride: dataform.IProjectConfig = {};

    if (argv[ProjectConfigOverride.warehouseOverrideOption.name]) {
      projectConfigOverride.warehouse = argv[ProjectConfigOverride.warehouseOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.defaultDatabaseOverrideOption.name]) {
      projectConfigOverride.defaultDatabase =
        argv[ProjectConfigOverride.defaultDatabaseOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.defaultSchemaOverrideOption.name]) {
      projectConfigOverride.defaultSchema =
        argv[ProjectConfigOverride.defaultSchemaOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.defaultLocationOverrideOption.name]) {
      projectConfigOverride.defaultLocation =
        argv[ProjectConfigOverride.defaultLocationOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.assertionSchemaOverrideOption.name]) {
      projectConfigOverride.assertionSchema =
        argv[ProjectConfigOverride.assertionSchemaOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.varsOverrideOption.name]) {
      projectConfigOverride.vars = argv[ProjectConfigOverride.varsOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.databaseSuffixOverrideOption.name]) {
      projectConfigOverride.databaseSuffix =
        argv[ProjectConfigOverride.databaseSuffixOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.schemaSuffixOverrideOption.name]) {
      projectConfigOverride.schemaSuffix =
        argv[ProjectConfigOverride.schemaSuffixOverrideOption.name];
    }
    if (argv[ProjectConfigOverride.tablePrefixOverrideOption.name]) {
      projectConfigOverride.schemaSuffix =
        argv[ProjectConfigOverride.tablePrefixOverrideOption.name];
    }
    return projectConfigOverride;
  }
}
