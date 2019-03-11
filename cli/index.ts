#!/usr/bin/env node
import * as fs from "fs";
import * as yargs from "yargs";
import * as path from "path";
import * as chokidar from "chokidar";
import * as protos from "@dataform/protos";
import { init, compile, build, run, table, query, utils } from "@dataform/api";

const addBuildYargs = (yargs: yargs.Argv) =>
  yargs
    .option("full-refresh", {
      describe: "If set, this will rebuild incremental tables from scratch",
      type: "boolean",
      default: false,
      alias: "fr"
    })
    .option("nodes", {
      describe: "A list of node names or patterns to run, which can include * wildcards",
      type: "array",
      alias: "n"
    })
    .option("include-deps", {
      describe: "If set, dependencies for selected nodes will also be run",
      type: "boolean",
      alias: "id"
    });

const parseBuildArgs = (argv: yargs.Arguments): protos.IRunConfig => ({
  fullRefresh: argv["full-refresh"],
  nodes: argv["nodes"],
  includeDependencies: argv["include-deps"]
});

const compileProject = (projectDir: string) => {
  return compile(projectDir)
    .then(graph => console.log(JSON.stringify(graph, null, 4)))
    .catch(e => console.log(e));
};

const RECOMPILE_DELAY = 2000;

yargs
  .command(
    "init [project-dir]",
    "Create a new dataform project in the current, or specified directory.",
    yargs =>
      yargs
        .option("warehouse", {
          describe: "The warehouse type. One of [bigquery, redshift]",
          default: "bigquery"
        })
        .option("gcloud-project-id", {
          describe: "Your Google Cloud Project ID"
        })
        .positional("project-dir", {
          describe: "The directory in which to create the Dataform project.",
          default: "."
        })
        .option("skip-install", {
          describe: "Whether to skip installing packages.",
          default: false
        }),
    argv => {
      init(
        path.resolve(argv["project-dir"]),
        {
          warehouse: argv["warehouse"],
          gcloudProjectId: argv["gcloud-project-id"]
        },
        argv["skip-install"]
      );
    }
  )
  .command(
    "compile [project-dir]",
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
    yargs =>
      yargs
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        })
        .option("watch", {
          describe: "Watch the changes in the project directory.",
          type: "boolean",
          default: false
        }),
    argv => {
      const projectDir = path.resolve(argv["project-dir"]);

      compileProject(projectDir).then(() => {
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
                  compileProject(projectDir).then(() => {
                    console.log("Watcher ready for changes...");
                    isCompiling = false;
                  });
                }
              }, RECOMPILE_DELAY);
            });
        }
      });
    }
  )
  .command(
    "build [project-dir]",
    "Build the dataform project. Produces JSON output describing the execution graph.",
    yargs =>
      addBuildYargs(yargs)
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        })
        .option("profile", {
          describe: "The location of the profile JSON file to run against",
          required: true
        }),
    argv => {
      const profile = utils.getProfile(argv["profile"]);
      compile(path.resolve(argv["project-dir"]))
        .then(graph => build(graph, parseBuildArgs(argv), profile))
        .then(result => console.log(JSON.stringify(result, null, 4)))
        .catch(e => console.log(e));
    }
  )
  .command(
    "run [project-dir]",
    "Build and run the dataform project with the provided options.",
    yargs =>
      addBuildYargs(yargs)
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        })
        .option("profile", {
          describe: "The location of the profile JSON file to run against",
          required: true
        })
        .option("result-path", {
          describe: "Path to save executed graph to JSON file",
          type: "string"
        }),
    argv => {
      console.log("Project status: starting...");
      const profile = utils.getProfile(argv["profile"]);

      compile(path.resolve(argv["project-dir"]))
        .then(graph => {
          console.log("Project status: build...");

          return build(graph, parseBuildArgs(argv), profile);
        })
        .then(graph => {
          const tasksAmount = graph.nodes.reduce((prev, item) => prev + item.tasks.length, 0);
          console.log(`Project status: ready for run ${graph.nodes.length} node(s) with ${tasksAmount} task(s)`);
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
  )
  .command(
    "tables-list",
    "Fetch available tables for the provided profile.",
    yargs =>
      yargs.option("profile", {
        describe: "The location of the profile JSON file to run against",
        required: true
      }),
    argv => {
      table
        .list(utils.getProfile(argv["profile"]))
        .then(tables => console.log(JSON.stringify(tables, null, 4)))
        .catch(e => console.log(e));
    }
  )
  .command(
    "tables-get <schema> <table>",
    "Fetch metadata for the given table",
    yargs =>
      yargs.option("profile", {
        describe: "The location of the profile JSON file to run against",
        required: true
      }),
    argv => {
      table
        .get(utils.getProfile(argv["profile"]), {
          schema: argv["schema"],
          name: argv["table"]
        })
        .then(schema => console.log(JSON.stringify(schema, null, 4)))
        .catch(e => console.log(e));
    }
  )
  .command(
    "query-compile <query> [project-dir]",
    "Compile the given query, evaluating project macros.",
    yargs =>
      yargs.positional("project-dir", {
        describe: "The directory of the Dataform project.",
        default: "."
      }),
    argv => {
      console.log(argv);
      query
        .compile(argv["query"], { projectDir: path.resolve(argv["project-dir"]) })
        .then(compiledQuery => console.log(compiledQuery))
        .catch(e => console.log(e));
    }
  )
  .command(
    "query-evaluate <query> [project-dir]",
    "Evaluate the query, checking it's valid against the warehouse.",
    yargs =>
      yargs
        .option("profile", {
          describe: "The location of the profile JSON file to run against",
          required: true
        })
        .positional("query", {
          type: "string",
          describe: "The query to evaluate."
        })
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        }),
    argv => {
      query
        .compile(argv["query"], { projectDir: path.resolve(argv["project-dir"]) })
        .then(compiledQuery => {
          // const profile = JSON.parse(fs.readFileSync(argv["profile"], "utf8"));
          const profile = utils.getProfile(argv["profile"]);
          if (profile.snowflake) {
            return console.log("Not implemented! You can try to use the web interface in your Snowflake profile");
          }

          return query.evaluate(profile, compiledQuery, {
            projectDir: path.resolve(argv["project-dir"])
          });
        })
        .catch(e => console.log(e));
    }
  )
  .command(
    "query-run <query> [project-dir]",
    "Execute the given query against the warehouse",
    yargs =>
      yargs
        .option("profile", {
          describe: "The location of the profile JSON file to run against",
          required: true
        })
        .positional("query", {
          describe: "The query to compile and run."
        })
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        }),
    argv => {
      const promise = query
        .run(utils.getProfile(argv["profile"]), argv["query"], {
          projectDir: path.resolve(argv["project-dir"])
        })
        .then(results => console.log(JSON.stringify(results, null, 4)))
        .catch(e => console.log(e));

      process.on("SIGINT", () => {
        if (promise.cancel) {
          promise.cancel();
          console.log("\nQuery execution cancelled!");
        }
      });
    }
  )
  .demandCommand(1, "You need at least one command before moving on").argv;
