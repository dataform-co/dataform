#!/usr/bin/env node
import { build, compile, init, run, table, utils } from "@dataform/api";
import { WarehouseTypes } from "@dataform/core/adapters";
import * as chokidar from "chokidar";
import * as fs from "fs";
import * as path from "path";
import * as yargs from "yargs";

const RECOMPILE_DELAY = 2000;

const projectDirOption = {
  name: "project-dir",
  option: {
    describe: "The Dataform project directory.",
    default: ".",
    coerce: fullyResolvePath
  }
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

// TODO: Should this be only set when "nodes" is also set? (using yargs 'implies')
const includeDepsOption = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected nodes will also be run.",
    type: "boolean"
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
    coerce: fullyResolvePath
  }
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
        // TODO: Should we error out if this is not provided when the warehouse is set to "bigquery",
        // or conversely if it is provided for other warehouse types?
        {
          name: "gcloud-project-id",
          option: {
            describe: "The Google Cloud Project ID to use when accessing bigquery."
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
      processFn: argv =>
        init(
          argv["project-dir"],
          {
            warehouse: argv["warehouse"],
            gcloudProjectId: argv["gcloud-project-id"]
          },
          argv["skip-install"]
        )
    },
    {
      format: "compile [project-dir]",
      description:
        "Compile the dataform project. Produces JSON output describing the non-executable graph.",
      positionalOptions: [projectDirOption],
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
      processFn: argv => {
        const projectDir = argv["project-dir"];
        const defaultSchemaOverride = argv["default-schema"];
        const assertionSchemaOverride = argv["assertion-schema"];

        compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride).then(() => {
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
              .on("ready", () => console.log("Watcher ready for changes..."))
              .on("error", error => console.error(`Watcher error: ${error}`))
              .on("all", () => {
                if (timeoutID || isCompiling) {
                  // don't recompile many times if we changed a lot of files
                  clearTimeout(timeoutID);
                } else {
                  console.log("Watcher recompile project...");
                }

                timeoutID = setTimeout(() => {
                  clearTimeout(timeoutID);

                  if (!isCompiling) {
                    // recompile project
                    isCompiling = true;
                    compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride).then(
                      () => {
                        console.log("Watcher ready for changes...");
                        isCompiling = false;
                      }
                    );
                  }
                }, RECOMPILE_DELAY);
              });
          }
        });
      }
    },
    {
      format: "build [project-dir]",
      description:
        "Build the dataform project. Produces JSON output describing the execution graph.",
      positionalOptions: [projectDirOption],
      options: [
        fullRefreshOption,
        nodesOption,
        includeDepsOption,
        defaultSchemaOption,
        assertionSchemaOption,
        credentialsOption
      ],
      processFn: argv => {
        compile({
          projectDir: argv["project-dir"],
          defaultSchemaOverride: argv["default-schema"],
          assertionSchemaOverride: argv["assertion-schema"]
        })
          .then(graph => {
            const projectConfig = require(path.resolve(argv["project-dir"], "dataform.json"));
            const credentials = utils.readCredentials(projectConfig.warehouse, argv["credentials"]);
            return build(
              graph,
              {
                fullRefresh: argv["full-refresh"],
                nodes: argv["nodes"],
                includeDependencies: argv["include-deps"]
              },
              credentials,
              projectConfig.warehouse
            );
          })
          .then(result => console.log(JSON.stringify(result, null, 4)))
          .catch(e => console.log(e));
      }
    },
    {
      format: "run [project-dir]",
      description: "Run the dataform project's scripts on the configured data warehouse.",
      positionalOptions: [projectDirOption],
      options: [
        fullRefreshOption,
        nodesOption,
        includeDepsOption,
        defaultSchemaOption,
        assertionSchemaOption,
        credentialsOption,
        {
          name: "result-path",
          option: {
            describe: "Optional path where executed graph JSON should be written.",
            type: "string"
          }
        }
      ],
      processFn: argv => {
        console.log("Project status: starting...");

        const projectConfig = require(path.resolve(argv["project-config"], "dataform.json"));
        const credentials = utils.readCredentials(projectConfig.warehouse, argv["credentials"]);

        compile({
          projectDir: argv["project-dir"],
          defaultSchemaOverride: argv["default-schema"],
          assertionSchemaOverride: argv["assertion-schema"]
        })
          .then(graph => {
            console.log("Project status: build...");

            return build(
              graph,
              {
                fullRefresh: argv["full-refresh"],
                nodes: argv["nodes"],
                includeDependencies: argv["include-deps"]
              },
              credentials,
              projectConfig.warehouse
            );
          })
          .then(graph => {
            const tasksAmount = graph.nodes.reduce((prev, item) => prev + item.tasks.length, 0);
            console.log(
              `Project status: ready for run ${
                graph.nodes.length
              } node(s) with ${tasksAmount} task(s)`
            );
            console.log("Project status: running...");

            const runner = run(graph, credentials);
            process.on("SIGINT", () => {
              runner.cancel();
            });
            return runner.resultPromise();
          })
          .then(result => {
            console.log("Project status: finished");
            const pathForResult = argv["result-path"];

            if (pathForResult) {
              const graph = JSON.stringify(result);
              fs.writeFileSync(pathForResult, graph);
              console.log(`Project status: executed graph is saved to file: "${pathForResult}"`);
            }

            console.log(JSON.stringify(result, null, 4));
          })
          .catch(e => console.log(e));
      }
    },
    {
      format: "listtables <warehouse>",
      description: "List tables on the configured data warehouse.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: argv => {
        table
          .list(utils.readCredentials(argv["warehouse"], argv["credentials"]), argv["warehouse"])
          .then(tables => console.log(JSON.stringify(tables, null, 4)))
          .catch(e => console.log(e));
      }
    },
    {
      format: "gettablemetadata <warehouse> <schema> <table>",
      description: "Fetch metadata for a specified table.",
      positionalOptions: [warehouseOption],
      options: [credentialsOption],
      processFn: argv => {
        table
          .get(utils.readCredentials(argv["warehouse"], argv["credentials"]), argv["warehouse"], {
            schema: argv["schema"],
            name: argv["table"]
          })
          .then(schema => console.log(JSON.stringify(schema, null, 4)))
          .catch(e => console.log(e));
      }
    }
  ],
  moreChaining: yargs => yargs.demandCommand(1, "Please choose a command.").argv
});

function fullyResolvePath(filePath: string) {
  filePath = path.resolve(filePath);
  if (!fs.existsSync(filePath)) {
    throw new Error(`${filePath} does not exist!`);
  }
  return filePath;
}

const compileProject = (
  projectDir: string,
  defaultSchemaOverride?: string,
  assertionSchemaOverride?: string
) => {
  return compile({ projectDir, defaultSchemaOverride, assertionSchemaOverride })
    .then(graph => console.log(JSON.stringify(graph, null, 4)))
    .catch(e => console.log(e));
};

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
  let yargsChain = yargs;
  for (let i = 0; i < command.positionalOptions.length; i++) {
    const positionalOption = command.positionalOptions[i];
    yargsChain = yargsChain.positional(positionalOption.name, positionalOption.option);
  }
  for (let i = 0; i < command.options.length; i++) {
    const option = command.options[i];
    yargsChain = yargsChain.option(option.name, option.option);
  }
  return yargsChain;
}
