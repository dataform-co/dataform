#!/usr/bin/env node
import * as fs from "fs";
import * as yargs from "yargs";
import * as path from "path";
import * as protos from "@dataform/protos";
import { init, compile, build, run, table, query } from "@dataform/api";

const addBuildYargs = (yargs: yargs.Argv) =>
  yargs
    .option("full-refresh", {
      describe: "If set, this will rebuild incremental tables from scratch",
      type: "boolean",
      default: false,
      alias: "fr"
    })
    .option("carry-on", {
      describe:
        "If set, when a task fails it won't stop dependencies from attempting to run",
      type: "boolean",
      default: false,
      alias: "co"
    })
    .option("nodes", {
      describe:
        "A list of node names or patterns to run, which can include * wildcards",
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
  carryOn: argv["carry-on"],
  nodes: argv["nodes"],
  includeDependencies: argv["include-deps"]
});

yargs
  .command(
    "init [project-dir]",
    "Create a new dataform project in the current, or specified directory.",
    yargs =>
      yargs
        .option("warehouse", {
          describe:
            "The warehouse type. One of [bigquery, redshift, snowflake, postgres]",
          default: "bigquery"
        })
        .positional("project-dir", {
          describe: "The directory in which to create the Dataform project.",
          default: "."
        }),
    argv => {
      init(path.resolve(argv["project-dir"]), argv["warehouse"]);
    }
  )
  .command(
    "compile [project-dir]",
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
    yargs =>
      yargs.positional("project-dir", {
        describe: "The directory of the Dataform project.",
        default: "."
      }),
    argv => {
      console.log(
        JSON.stringify(compile(path.resolve(argv["project-dir"])), null, 4)
      );
    }
  )
  .command(
    "build [project-dir]",
    "Build the dataform project. Produces JSON output describing the execution graph.",
    yargs =>
      addBuildYargs(yargs).positional("project-dir", {
        describe: "The directory of the Dataform project.",
        default: "."
      }),
    argv => {
      console.log(
        JSON.stringify(
          build(
            compile(path.resolve(argv["project-dir"])),
            parseBuildArgs(argv)
          ),
          null,
          4
        )
      );
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
        }),
    argv => {
      var runner = run(
        build(compile(path.resolve(argv["project-dir"])), parseBuildArgs(argv)),
        protos.Profile.create(
          JSON.parse(fs.readFileSync(argv["profile"], "utf8"))
        )
      );
      runner
        .resultPromise()
        .then(result => console.log(JSON.stringify(result, null, 4)))
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
        .list(
          protos.Profile.create(
            JSON.parse(fs.readFileSync(argv["profile"], "utf8"))
          )
        )
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
        .get(
          protos.Profile.create(
            JSON.parse(fs.readFileSync(argv["profile"], "utf8"))
          ),
          { schema: argv["schema"], name: argv["table"] }
        )
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
      console.log(
        query.compile(argv["query"], path.resolve(argv["project-dir"]))
      );
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
        .positional("project-dir", {
          describe: "The directory of the Dataform project.",
          default: "."
        }),
    argv => {
      query
        .run(
          protos.Profile.create(
            JSON.parse(fs.readFileSync(argv["profile"], "utf8"))
          ),
          argv["query"],
          path.resolve(argv["project-dir"])
        )
        .then(results => console.log(JSON.stringify(results, null, 4)))
        .catch(e => console.log(e));
    }
  ).argv;
