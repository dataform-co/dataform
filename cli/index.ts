#!/usr/bin/env node
import { build, compile, init, run, table, utils } from "@dataform/api";
import { prettyJsonStringify } from "@dataform/api/utils";
import { WarehouseType } from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import * as chokidar from "chokidar";
import * as fs from "fs";
import * as path from "path";
import * as readlineSync from "readline-sync";
import * as yargs from "yargs";

const RECOMPILE_DELAY = 2000;

// Uses ANSI escape color codes.
// https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
const coloredOutput = (output: string, ansiColorCode: number) =>
  `\x1b[${ansiColorCode}m${output}\x1b[0m`;
const commandOutput = (output: string) => coloredOutput(output, 34);
const errorOutput = (output: string) => coloredOutput(output, 91);

const writeStdOut = (output: string) => process.stdout.write(output + "\n");
const writeStdErr = (output: string) => process.stderr.write(output + "\n");

const projectDirOption = {
  name: "project-dir",
  option: {
    describe: "The Dataform project directory.",
    default: ".",
    coerce: path.resolve
  }
};

const projectDirMustExistOption = {
  ...projectDirOption,
  check: (argv: yargs.Arguments) => assertPathExists(argv["project-dir"])
};

const fullRefreshOption = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

const nodesOption = {
  name: "nodes",
  option: {
    describe: "A list of node names or patterns to run. Can include '*' wildcards.",
    type: "array"
  }
};

const includeDepsOption = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected nodes will also be run.",
    type: "boolean"
  },
  // It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
  check: (argv: yargs.Arguments) => {
    if (argv.include_deps && !argv.nodes) {
      throw new Error("The --include_deps flag should only be supplied along with --nodes.");
    }
  }
};

const defaultSchemaOption = {
  name: "default-schema",
  option: {
    describe: "An optional default schema name override."
  }
};

const assertionSchemaOption = {
  name: "assertion-schema",
  option: {
    describe: "An optional assertion schema name override."
  }
};

const credentialsOption = {
  name: "credentials",
  option: {
    describe: "The location of the credentials JSON file to use.",
    default: ".df-credentials.json",
    coerce: path.resolve
  },
  check: (argv: yargs.Arguments) => assertPathExists(argv.credentials)
};

const warehouseOption = {
  name: "warehouse",
  option: {
    describe: "The project's data warehouse type.",
    choices: Object.keys(WarehouseType).map(warehouseType => WarehouseType[warehouseType])
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
              throw new Error("The --gcloud-project-id flag is required for BigQuery projects.");
            }
          }
        },
        {
          name: "skip-install",
          option: {
            describe: "Whether to skip installing NPM packages.",
            default: false
          }
        }
      ],
      processFn: async argv => {
        const result = await init(
          argv["project-dir"],
          {
            warehouse: argv.warehouse,
            gcloudProjectId: argv["gcloud-project-id"]
          },
          argv["skip-install"]
        );

        writeStdOut(commandOutput("Directories created:"));
        result.dirsCreated.forEach(dir => writeStdOut(dir));
        writeStdOut(commandOutput("Files written:"));
        result.filesWritten.forEach(file => writeStdOut(file));
        if (result.installedNpmPackages) {
          writeStdOut(commandOutput("NPM packages successfully installed."));
        }
      }
    },
    {
      format: "init-creds <warehouse> [project-dir]",
      description:
        "Creates a .df-credentials.json file for dataform to use when accessing your warehouse.",
      positionalOptions: [warehouseOption, projectDirMustExistOption],
      options: [],
      processFn: argv => {
        const credentials = () => {
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
            case "snowflake": {
              return getSnowflakeCredentials();
            }
            default: {
              throw new Error(`Unrecognized warehouse type ${argv.warehouse}`);
            }
          }
        };
        fs.writeFileSync(
          path.resolve(argv["project-dir"], ".df-credentials.json"),
          prettyJsonStringify(credentials())
        );
      }
    },
    {
      format: "compile [project-dir]",
      description:
        "Compile the dataform project. Produces JSON output describing the non-executable graph.",
      positionalOptions: [projectDirMustExistOption],
      options: [
        defaultSchemaOption,
        assertionSchemaOption,
        {
          name: "watch",
          option: {
            describe: "Whether to watch the changes in the project directory.",
            type: "boolean",
            default: false
          }
        }
      ],
      processFn: async argv => {
        const projectDir = argv["project-dir"];
        const defaultSchemaOverride = argv["default-schema"];
        const assertionSchemaOverride = argv["assertion-schema"];

        await compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride);

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

          // Add event listeners.
          watcher
            .on("ready", () => writeStdOut(commandOutput("Watching for changes...")))
            .on("error", error => writeStdErr(errorOutput(`Error: ${error}`)))
            .on("all", () => {
              if (timeoutID || isCompiling) {
                // don't recompile many times if we changed a lot of files
                clearTimeout(timeoutID);
              } else {
                writeStdOut(commandOutput("Recompiling project..."));
              }

              timeoutID = setTimeout(async () => {
                clearTimeout(timeoutID);

                if (!isCompiling) {
                  isCompiling = true;
                  await compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride);
                  writeStdOut(commandOutput("Watching for changes..."));
                  isCompiling = false;
                }
              }, RECOMPILE_DELAY);
            });
        }
      }
    },
    {
      format: "build [project-dir]",
      description:
        "Build the dataform project using the current warehouse configuration to compute final SQL.",
      positionalOptions: [projectDirMustExistOption],
      options: [
        fullRefreshOption,
        nodesOption,
        includeDepsOption,
        defaultSchemaOption,
        assertionSchemaOption,
        credentialsOption
      ],
      processFn: async argv => {
        const compiledGraph = await compile({
          projectDir: argv["project-dir"],
          defaultSchemaOverride: argv["default-schema"],
          assertionSchemaOverride: argv["assertion-schema"]
        });
        if (compiledGraph.graphErrors) {
          logGraphErrors(compiledGraph.graphErrors);
          return;
        }
        const executionGraph = await build(
          compiledGraph,
          {
            fullRefresh: argv["full-refresh"],
            nodes: argv.nodes,
            includeDependencies: argv["include-deps"]
          },
          utils.readCredentials(compiledGraph.projectConfig.warehouse, argv.credentials)
        );
        writeStdOut(prettyJsonStringify(executionGraph));
      }
    },
    {
      format: "run [project-dir]",
      description: "Run the dataform project's scripts on the configured data warehouse.",
      positionalOptions: [projectDirMustExistOption],
      options: [
        fullRefreshOption,
        nodesOption,
        includeDepsOption,
        defaultSchemaOption,
        assertionSchemaOption,
        credentialsOption
      ],
      processFn: async argv => {
        const compiledGraph = await compile({
          projectDir: argv["project-dir"],
          defaultSchemaOverride: argv["default-schema"],
          assertionSchemaOverride: argv["assertion-schema"]
        });
        if (compiledGraph.graphErrors) {
          logGraphErrors(compiledGraph.graphErrors);
          return;
        }
        const credentials = utils.readCredentials(
          compiledGraph.projectConfig.warehouse,
          argv.credentials
        );
        const executionGraph = await build(
          compiledGraph,
          {
            fullRefresh: argv["full-refresh"],
            nodes: argv.nodes,
            includeDependencies: argv["include-deps"]
          },
          credentials
        );

        const runner = run(executionGraph, credentials);
        process.on("SIGINT", () => {
          runner.cancel();
        });
        const executedGraph = await runner.resultPromise();
        const prettyPrintedExecutedGraph = prettyJsonStringify(executedGraph);
        writeStdOut(prettyPrintedExecutedGraph);
        if (!executedGraph.ok) {
          executedGraph.nodes
            .filter(node => node.status === dataform.NodeExecutionStatus.FAILED)
            .forEach(node => {
              writeStdErr(errorOutput(`Execution failed on node "${node.name}":`));
              node.tasks.filter(task => !task.ok).forEach(task => {
                writeStdErr(
                  errorOutput(`Statement "${task.task.statement}" failed with error: ${task.error}`)
                );
              });
            });
        }
      }
    },
    {
      format: "listtables <warehouse>",
      description: "List tables on the configured data warehouse.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: async argv => {
        const tables = await table.list(
          utils.readCredentials(argv.warehouse, argv.credentials),
          argv.warehouse
        );
        tables.forEach(table => writeStdOut(`${table.schema}.${table.name}`));
      }
    },
    {
      format: "gettablemetadata <warehouse> <schema> <table>",
      description: "Fetch metadata for a specified table.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: async argv => {
        const tableMetadata = await table.get(
          utils.readCredentials(argv.warehouse, argv.credentials),
          argv.warehouse,
          {
            schema: argv.schema,
            name: argv.table
          }
        );
        writeStdOut(prettyJsonStringify(tableMetadata));
      }
    }
  ]
})
  .scriptName("dataform")
  .strict()
  .recommendCommands()
  .help("help")
  .fail((msg, err) => {
    writeStdErr(
      errorOutput(`Dataform encountered an error: ${err ? err.stack || err.message : msg}`)
    );
    process.exit(1);
  })
  .demandCommand(1, "Please choose a command.").argv;

function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
}

function getBigQueryCredentials(): dataform.IBigQuery {
  writeStdOut(
    commandOutput(
      "Please follow the instructions at https://docs.dataform.co/platform_guides/set_up_datawarehouse/ to create \n" +
        "and download a private key from the Google Cloud Console in JSON format.\n" +
        "(You can delete this file after credential initialization is complete.)\n"
    )
  );
  const cloudCredentialsPath = path.resolve(
    readlineSync.question(commandOutput("Enter the path to your Google Cloud private key file:\n"))
  );
  if (!fs.existsSync(cloudCredentialsPath)) {
    throw new Error(`Google Cloud private key file "${cloudCredentialsPath}" does not exist!`);
  }
  const cloudCredentials = require(cloudCredentialsPath);
  const locationIndex = readlineSync.keyInSelect(
    ["US (default)", "EU"],
    "Enter the location of your datasets:\n",
    {
      cancel: false
    }
  );
  return {
    projectId: cloudCredentials.project_id,
    credentials: fs.readFileSync(cloudCredentialsPath, "utf8"),
    location: locationIndex === 0 ? "US" : "EU"
  };
}

function getRedshiftCredentials() {
  return getJdbcCredentials(
    "Enter the hostname of your Redshift instance (in the form '[name].[id].[region].redshift.amazonaws.com'):\n",
    5439
  );
}

function getPostgresCredentials() {
  return getJdbcCredentials("Enter the hostname of your Postgres database:\n", 5432);
}

function getJdbcCredentials(hostQuestion: string, defaultPort: number): dataform.IJDBC {
  const host = readlineSync.question(commandOutput(hostQuestion));
  const port = readlineSync.questionInt(
    commandOutput(`Enter the port that Dataform should connect to (usually ${defaultPort}):\n`)
  );
  const username = readlineSync.question(commandOutput("Enter your database username:\n"));
  const password = readlineSync.question(commandOutput("Enter your database password:\n"), {
    hideEchoBack: true
  });
  const databaseName = readlineSync.question(commandOutput("Enter the database name:\n"));
  return {
    host,
    port,
    username,
    password,
    databaseName
  };
}

function getSnowflakeCredentials(): dataform.ISnowflake {
  const accountId = readlineSync.question(
    commandOutput(
      "Enter your Snowflake account identifier, including region (for example 'myaccount.us-east-1'):\n"
    )
  );
  const role = readlineSync.question(commandOutput("Enter your database role:\n"));
  const username = readlineSync.question(commandOutput("Enter your database username:\n"));
  const password = readlineSync.question(commandOutput("Enter your database password:\n"), {
    hideEchoBack: true
  });
  const databaseName = readlineSync.question(commandOutput("Enter the database name:\n"));
  const warehouse = readlineSync.question(commandOutput("Enter your warehouse name:\n"));
  return {
    accountId,
    role,
    username,
    password,
    databaseName,
    warehouse
  };
}

async function compileProject(
  projectDir: string,
  defaultSchemaOverride?: string,
  assertionSchemaOverride?: string
) {
  const graph = await compile({ projectDir, defaultSchemaOverride, assertionSchemaOverride });
  if (graph.graphErrors) {
    logGraphErrors(graph.graphErrors);
  } else {
    writeStdOut(prettyJsonStringify(graph));
  }
}

function logGraphErrors(graphErrors: dataform.IGraphErrors) {
  writeStdErr(errorOutput("Compiled graph contains errors."));
  if (graphErrors.compilationErrors) {
    writeStdErr(errorOutput("Compilation errors:"));
    graphErrors.compilationErrors.forEach(compileError => {
      writeStdErr(
        errorOutput(`${compileError.fileName}: ${compileError.stack || compileError.message}`)
      );
    });
  }
  if (graphErrors.validationErrors) {
    writeStdErr(errorOutput("Validation errors:"));
    graphErrors.validationErrors.forEach(validationError => {
      writeStdErr(errorOutput(`${validationError.nodeName}: ${validationError.message}`));
    });
  }
}

interface ICli {
  commands: ICommand[];
}

interface ICommand {
  format: string;
  description: string;
  positionalOptions: Array<INamedOption<yargs.PositionalOptions>>;
  options: Array<INamedOption<yargs.Options>>;
  processFn: (argv) => any;
}

interface INamedOption<T> {
  name: string;
  option: T;
  check?: (args: yargs.Arguments) => void;
}

function createYargsCli(cli: ICli) {
  let yargsChain = yargs;
  for (const command of cli.commands) {
    yargsChain = yargsChain.command(
      command.format,
      command.description,
      yargsChainer => createOptionsChain(yargsChainer, command),
      command.processFn
    );
  }
  return yargsChain;
}

function createOptionsChain(yargsChain: yargs.Argv, command: ICommand) {
  const checks: Array<(args: yargs.Arguments) => void> = [];

  for (const positionalOption of command.positionalOptions) {
    yargsChain = yargsChain.positional(positionalOption.name, positionalOption.option);
    if (positionalOption.check) {
      checks.push(positionalOption.check);
    }
  }
  for (const option of command.options) {
    yargsChain = yargsChain.option(option.name, option.option);
    if (option.check) {
      checks.push(option.check);
    }
  }
  yargsChain = yargsChain.check(argv => {
    checks.forEach(check => check(argv));
    return true;
  });
  return yargsChain;
}
