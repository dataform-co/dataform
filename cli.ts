#!/usr/bin/env node
import * as fs from "fs";
import * as util from "util";
import * as yargs from "yargs";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import * as protos from "./protos";
import * as utils from "./utils";
import * as runners from "./runners";
import { Executor } from "./executor";
import * as commands from "./commands";

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
        "If set, when a task fails it won't stop dependencies from attempting to run.",
      type: "boolean",
      default: false,
      alias: "co"
    })
    .option("retries", {
      describe: "If set, failing tasks will be retried this many times.",
      type: "number",
      default: false,
      alias: "r"
    })
    .option("nodes", {
      describe: "A list of computation nodes to run. Defaults to all nodes",
      type: "array"
    })
    .option("include-deps", {
      describe: "If set, dependencies for selected nodes will also be run",
      type: "boolean",
      alias: "id"
    });

const parseBuildArgs = (argv: yargs.Arguments): protos.IRunConfig => ({
  fullRefresh: argv["full-refresh"],
  carryOn: argv["carry-on"],
  retries: argv["retries"],
  nodes: argv["nodes"],
  includeDependencies: argv["include-deps"]
});

yargs
  .option("project-dir", {
    describe: "The directory of the dataform project to run against",
    default: "."
  })
  .command(
    "init",
    "Create a new dataform project in the current, or specified directory.",
    yargs => yargs,
    argv => {
      commands.init(argv["project-dir"]);
    }
  )
  .command(
    "compile",
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
    yargs => yargs,
    argv => {
      console.log(
        JSON.stringify(commands.compile(argv["project-dir"]), null, 4)
      );
    }
  )
  .command(
    "build",
    "Build the dataform project. Produces JSON output describing the execution graph.",
    yargs => addBuildYargs(yargs),
    argv => {
      console.log(
        JSON.stringify(
          commands.build(argv["project-dir"], parseBuildArgs(argv)),
          null,
          4
        )
      );
    }
  )
  .command(
    "run",
    "Build and run the dataform project, with the provided options.",
    yargs =>
      addBuildYargs(yargs).option("profile", {
        describe: "The location of the profile file to run against"
      }),
    argv => {
      commands
        .run(
          commands.build(argv["project-dir"], parseBuildArgs(argv)),
          protos.Profile.create(
            JSON.parse(fs.readFileSync(argv["profile"], "utf8"))
          )
        )
        .then(result => console.log(JSON.stringify(result, null, 4)));
    }
  ).argv;
