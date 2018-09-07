#!/usr/bin/env node
import * as fs from "fs";
import * as util from "util";
import * as yargs from "yargs";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import * as protos from "./protos";
import * as utils from "./utils";

const argv = yargs
  .option("project-dir", { describe: "The directory of the dataform project to run against", default: "." })
  .command(
    "compile",
    "Compile the dataform project. Produces JSON output describing the entire computation graph.",
    yargs => yargs,
    argv => compile(argv["project-dir"])
  )
  .command(
    "run",
    "Will run the computation graph, with the provided options.",
    yargs =>
      yargs
        .option("dry-run", {
          describe: "Prints the executable computation graph without actually running it",
          type: "boolean",
          default: false,
          alias: "dr"
        })
        .option("full-refresh", {
          describe: "If set, this will rebuild incremental tables from scratch",
          type: "boolean",
          default: false,
          alias: "fr"
        })
        .option("carry-on", {
          describe: "If set, when a task fails it won't stop dependencies from attempting to run.",
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
        .option("profile", { describe: "The location of the profile file to run against" })
        .option("nodes", { describe: "A list of computation nodes to run. Defaults to all nodes", type: "array" })
        .option("include-deps", {
          describe: "If set, dependencies for selected nodes will also be run",
          type: "boolean",
          alias: "id"
        }),
    argv => {
      run(
        argv["project-dir"],
        argv["dry-run"],
        argv["full-refresh"],
        argv["profile"],
        argv["nodes"],
        argv["include-deps"],
        argv["carry-on"],
        argv["retries"]
      );
    }
  ).argv;

  const vm = new NodeVM({
    timeout: 5000,
    wrapper: "none",
    require: {
      context: "sandbox",
      root: "./",
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: (code, file) => {
      if (file.includes(".sql")) {
        return utils.compileSql(code, file);
      } else {
        return code;
      }
    }
  });

function run(
  projectDir: string,
  dryRun: boolean,
  fullRefresh: boolean,
  profile: string,
  nodes: string[],
  includeDeps: boolean,
  carryOn: boolean,
  retries: number
) {
  var runConfig: protos.IRunConfig = {
    fullRefresh: fullRefresh,
    includeDependencies: includeDeps,
    nodes: nodes
  };

}

function compile(projectDir: string, runOptions?: protos.IRunConfig) {

  var projectConfig = protos.ProjectConfig.create({
    buildPaths: ["models/*"],
    includePaths: ["includes/*"]
  });

  var projectConfigPath = path.join(projectDir, "dataform.json");

  if (fs.existsSync(projectConfigPath)) {
    Object.assign(projectConfig, JSON.parse(fs.readFileSync(projectConfigPath, "utf8")));
  }

  var packageJsonPath = path.join(projectDir, "package.json");
  var packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  var includePaths = glob.sync(projectConfig.includePaths[0], { cwd: projectDir });
  var modelPaths = glob.sync(projectConfig.buildPaths[0], { cwd: projectDir });

  var packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `global.${utils.variableNameFriendly(packageName)} = require("${packageName}");`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `global.${utils.baseFilename(path)} = require("./${path}");`;
    })
    .join("\n");
  var modelRequires = modelPaths
    .map(path => {
      return `require("./${path}");`;
    })
    .join("\n");

  var mainScript = `
const dft = require("dft");
dft.init(require("./dataform.json"));
${packageRequires}
${includeRequires}
${modelRequires}
return dft.build({});`;

  var output = vm.run(mainScript, path.resolve(path.join(projectDir, "index.js")));

  console.log(JSON.stringify(output, null, 4));
}
