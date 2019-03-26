#!/usr/bin/env node
import { build, compile, init, query, run, table, utils } from "@dataform/api";
import * as protos from "@dataform/protos";
import * as chokidar from "chokidar";
import * as fs from "fs";
import * as path from "path";
import * as yargs from "yargs";

const RECOMPILE_DELAY = 2000;

const commonBuildOptions: INamedOption[] = [
  {
    name: "full-refresh",
    option: {
      describe: "If set, this will rebuild incremental tables from scratch",
      type: "boolean",
      default: false,
      alias: "fr"
    }
  },
  {
    name: "nodes",
    option: {
      describe: "A list of node names or patterns to run, which can include * wildcards",
      type: "array",
      alias: "n"
    }
  },
  {
    name: "include-deps",
    option: {
      describe: "If set, dependencies for selected nodes will also be run",
      type: "boolean",
      alias: "id"
    }
  }
];

createYargsCli({
  commands: [
    {
      format: "init [project-dir]",
      description: "Create a new dataform project in the current, or specified directory.",
      positionalOptions: [
        {
          name: "project-dir",
          option: {
            describe: "The directory in which to create the Dataform project.",
            default: "."
          }
        }
      ],
      options: [
        {
          name: "warehouse",
          option: {
            describe: "The warehouse type. One of [bigquery, redshift, snowflake]",
            required: true
          }
        },
        {
          name: "gcloud-project-id",
          option: {
            describe: "Your Google Cloud Project ID"
          }
        },
        {
          name: "skip-install",
          option: {
            describe: "Whether to skip installing packages.",
            default: false
          }
        }
      ],
      processFn: argv =>
        init(
          path.resolve(argv["project-dir"]),
          {
            warehouse: argv.warehouse,
            gcloudProjectId: argv["gcloud-project-id"]
          },
          argv["skip-install"]
        )
    },
    {
      format: "compile [project-dir]",
      description:
        "Compile the dataform project. Produces JSON output describing the non-executable graph.",
      positionalOptions: [
        {
          name: "project-dir",
          option: {
            describe: "The directory of the Dataform project.",
            default: "."
          }
        }
      ],
      options: [
        {
          name: "default-schema",
          option: {
            describe: "An optional default schema name override"
          }
        },
        {
          name: "assertion-schema",
          option: {
            describe: "An optional assertion schema name override"
          }
        },
        {
          name: "watch",
          option: {
            describe: "Watch the changes in the project directory.",
            type: "boolean",
            default: false
          }
        }
      ],
      processFn: argv => {
        const projectDir = path.resolve(argv["project-dir"]);
        const defaultSchemaOverride = !!argv["default-schema-override"]
          ? path.resolve(argv["default-schema-override"])
          : "";
        const assertionSchemaOverride = !!argv["assertion-schema-override"]
          ? path.resolve(argv["assertion-schema-override"])
          : "";

        compileProject(projectDir, defaultSchemaOverride, assertionSchemaOverride).then(() => {
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
      positionalOptions: [
        {
          name: "project-dir",
          option: {
            describe: "The directory of the Dataform project.",
            default: "."
          }
        }
      ],
      options: [
        ...commonBuildOptions,
        {
          name: "profile",
          option: {
            describe: "The location of the profile JSON file to run against",
            required: true
          }
        }
      ],
      processFn: argv => {
        const profile = utils.readProfile(argv.profile);
        const defaultSchemaOverride = !!argv["default-schema-override"]
          ? path.resolve(argv["default-schema-override"])
          : "";
        const assertionSchemaOverride = !!argv["assertion-schema-override"]
          ? path.resolve(argv["assertion-schema-override"])
          : "";

        compile({
          projectDir: path.resolve(argv["project-dir"]),
          defaultSchemaOverride,
          assertionSchemaOverride
        })
          .then(graph => build(graph, parseBuildArgs(argv), profile))
          .then(result => console.log(JSON.stringify(result, null, 4)))
          .catch(e => console.log(e));
      }
    },
    {
      format: "run [project-dir]",
      description: "Build and run the dataform project with the provided options.",
      positionalOptions: [
        {
          name: "project-dir",
          option: {
            describe: "The directory of the Dataform project.",
            default: "."
          }
        }
      ],
      options: [
        ...commonBuildOptions,
        {
          name: "profile",
          option: {
            describe: "The location of the profile JSON file to run against",
            required: true
          }
        },
        {
          name: "result-path",
          option: {
            describe: "Path to save executed graph to JSON file",
            type: "string"
          }
        }
      ],
      processFn: argv => {
        console.log("Project status: starting...");
        const profile = utils.readProfile(argv.profile);
        const defaultSchemaOverride = !!argv["default-schema-override"]
          ? path.resolve(argv["default-schema-override"])
          : "";
        const assertionSchemaOverride = !!argv["assertion-schema-override"]
          ? path.resolve(argv["assertion-schema-override"])
          : "";

        compile({
          projectDir: path.resolve(argv["project-dir"]),
          defaultSchemaOverride,
          assertionSchemaOverride
        })
          .then(graph => {
            console.log("Project status: build...");

            return build(graph, parseBuildArgs(argv), profile);
          })
          .then(graph => {
            const tasksAmount = graph.nodes.reduce((prev, item) => prev + item.tasks.length, 0);
            console.log(
              `Project status: ready for run ${
                graph.nodes.length
              } node(s) with ${tasksAmount} task(s)`
            );
            console.log("Project status: running...");

            const runner = run(graph, profile);
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
      format: "tables-list",
      description: "Fetch available tables for the provided profile.",
      positionalOptions: [],
      options: [
        {
          name: "profile",
          option: {
            describe: "The location of the profile JSON file to run against",
            required: true
          }
        }
      ],
      processFn: argv => {
        table
          .list(utils.readProfile(argv.profile))
          .then(tables => console.log(JSON.stringify(tables, null, 4)))
          .catch(e => console.log(e));
      }
    },
    {
      format: "tables-get <schema> <table>",
      description: "Fetch metadata for the given table",
      positionalOptions: [],
      options: [
        {
          name: "profile",
          option: {
            describe: "The location of the profile JSON file to run against",
            required: true
          }
        }
      ],
      processFn: argv => {
        table
          .get(utils.readProfile(argv.profile), {
            schema: argv.schema,
            name: argv.table
          })
          .then(schema => console.log(JSON.stringify(schema, null, 4)))
          .catch(e => console.log(e));
      }
    }
  ],
  moreChaining: yargs =>
    yargs.demandCommand(1, "You need at least one command before moving on").argv
});

const parseBuildArgs = (argv: yargs.Arguments): protos.IRunConfig => ({
  fullRefresh: argv["full-refresh"],
  nodes: argv.nodes,
  includeDependencies: argv["include-deps"]
});

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
  positionalOptions: INamedPositionalOption[];
  options: INamedOption[];
  processFn: (argv) => any;
}

interface INamedPositionalOption {
  name: string;
  option: yargs.PositionalOptions;
}

interface INamedOption {
  name: string;
  option: yargs.Options;
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
