#!/usr/bin/env node
import { build, compile, init, run, table, utils } from "@dataform/api";
import { prettyJsonStringify } from "@dataform/api/utils";
import { WarehouseTypes } from "@dataform/core/adapters";
import * as chokidar from "chokidar";
import * as fs from "fs";
import * as path from "path";
import * as yargs from "yargs";
import { dataform } from "df/protos";

const RECOMPILE_DELAY = 2000;

// Uses ANSI escape color codes.
// https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
const colorFmtString = (ansiColorCode: number) => `\x1b[${ansiColorCode}m%s\x1b[0m`;
const commandOutputFmtString = colorFmtString(34);
const errorOutputFmtString = colorFmtString(91);

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
    if (argv["include_deps"] && !argv["nodes"]) {
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
  check: (argv: yargs.Arguments) => assertPathExists(argv["credentials"])
};

const warehouseOption = {
  name: "warehouse",
  option: {
    describe: "The project's data warehouse type.",
    choices: Object.keys(WarehouseTypes).map(warehouseType => WarehouseTypes[warehouseType])
  }
};

createYargsCli({
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
            if (argv["gcloud-project-id"] && argv["warehouse"] !== "bigquery") {
              throw new Error("The --gcloud-project-id flag is only used for BigQuery projects.");
            }
            if (!argv["gcloud-project-id"] && argv["warehouse"] === "bigquery") {
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
            warehouse: argv["warehouse"],
            gcloudProjectId: argv["gcloud-project-id"]
          },
          argv["skip-install"]
        );

        console.log(commandOutputFmtString, "Directories created:");
        result.dirsCreated.forEach(dir => console.log(dir));
        console.log(commandOutputFmtString, "Files written:");
        result.filesWritten.forEach(file => console.log(file));
        if (result.installedNpmPackages) {
          console.log(commandOutputFmtString, "NPM packages successfully installed.");
        }
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

        if (argv["watch"]) {
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
            .on("ready", () => console.log(commandOutputFmtString, "Watching for changes..."))
            .on("error", error => console.error(`Error: ${error}`))
            .on("all", () => {
              if (timeoutID || isCompiling) {
                // don't recompile many times if we changed a lot of files
                clearTimeout(timeoutID);
              } else {
                console.log(commandOutputFmtString, "Recompiling project...");
              }

              timeoutID = setTimeout(() => {
                clearTimeout(timeoutID);

                if (!isCompiling) {
                  // recompile project
                  isCompiling = true;
                  compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride).then(
                    () => {
                      console.log(commandOutputFmtString, "Watching for changes...");
                      isCompiling = false;
                    }
                  );
                }
              }, RECOMPILE_DELAY);
            });
        }
      }
    },
    {
      format: "build [project-dir]",
      description:
        "Build the dataform project, using the current state of the configured warehouse to compute final SQL.",
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
            nodes: argv["nodes"],
            includeDependencies: argv["include-deps"]
          },
          utils.readCredentials(compiledGraph.projectConfig.warehouse, argv["credentials"])
        );
        console.log(prettyJsonStringify(executionGraph));
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
          argv["credentials"]
        );
        const executionGraph = await build(
          compiledGraph,
          {
            fullRefresh: argv["full-refresh"],
            nodes: argv["nodes"],
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
        console.log(prettyPrintedExecutedGraph);
        if (!executedGraph.ok) {
          executedGraph.nodes
            .filter(node => node.status === dataform.NodeExecutionStatus.FAILED)
            .forEach(node => {
              console.log(errorOutputFmtString, `Execution failed on node "${node.name}":`);
              node.tasks.filter(task => !task.ok).forEach(task => {
                console.log(
                  errorOutputFmtString,
                  `Statement "${task.task.statement}" failed with error: ${task.error}`
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
          utils.readCredentials(argv["warehouse"], argv["credentials"]),
          argv["warehouse"]
        );
        tables.forEach(table => console.log(`${table.schema}.${table.name}`));
      }
    },
    {
      format: "gettablemetadata <warehouse> <schema> <table>",
      description: "Fetch metadata for a specified table.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: async argv => {
        const tableMetadata = await table.get(
          utils.readCredentials(argv["warehouse"], argv["credentials"]),
          argv["warehouse"],
          {
            schema: argv["schema"],
            name: argv["table"]
          }
        );
        console.log(prettyJsonStringify(tableMetadata));
      }
    }
  ],
  moreChaining: yargs =>
    yargs
      .scriptName("dataform")
      .strict()
      .recommendCommands()
      .help("help")
      .fail((msg, err) => {
        console.log(
          errorOutputFmtString,
          `Dataform encountered an error: ${err ? err.stack || err.message : msg}`
        );
        process.exit(1);
      })
      .demandCommand(1, "Please choose a command.").argv
});

function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
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
    console.log(prettyJsonStringify(graph));
  }
}

function logGraphErrors(graphErrors: dataform.IGraphErrors) {
  console.log(errorOutputFmtString, "Compiled graph contains errors.");
  if (graphErrors.compilationErrors) {
    console.log(errorOutputFmtString, "Compilation errors:");
    graphErrors.compilationErrors.forEach(compileError => {
      console.log(
        errorOutputFmtString,
        `${compileError.fileName}: ${compileError.stack || compileError.message}`
      );
    });
  }
  if (graphErrors.validationErrors) {
    console.log(errorOutputFmtString, "Validation errors:");
    graphErrors.validationErrors.forEach(validationError => {
      console.log(errorOutputFmtString, `${validationError.nodeName}: ${validationError.message}`);
    });
  }
}

interface ICli {
  commands: ICommand[];
  moreChaining?: (yargs: yargs.Argv) => any;
}

interface ICommand {
  format: string;
  description: string;
  positionalOptions: INamedOption<yargs.PositionalOptions>[];
  options: INamedOption<yargs.Options>[];
  processFn: (argv) => any;
}

interface INamedOption<T> {
  name: string;
  option: T;
  check?: (args: yargs.Arguments) => void;
}

function createYargsCli(cli: ICli) {
  let yargsChain = yargs;
  for (let i = 0; i < cli.commands.length; i++) {
    const command = cli.commands[i];
    yargsChain = yargsChain.command(
      command.format,
      command.description,
      yargs => createOptionsChain(yargs, command),
      command.processFn
    );
  }

  if (cli.moreChaining) {
    return cli.moreChaining(yargsChain);
  }
  return yargsChain;
}

function createOptionsChain(yargs: yargs.Argv, command: ICommand) {
  const checks: ((args: yargs.Arguments) => void)[] = [];

  let yargsChain = yargs;
  for (let i = 0; i < command.positionalOptions.length; i++) {
    const positionalOption = command.positionalOptions[i];
    yargsChain = yargsChain.positional(positionalOption.name, positionalOption.option);
    if (positionalOption.check) {
      checks.push(positionalOption.check);
    }
  }
  for (let i = 0; i < command.options.length; i++) {
    const option = command.options[i];
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
