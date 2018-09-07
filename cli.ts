#!/usr/bin/env node
import * as fs from "fs";
import * as util from "util";
import * as yargs from "yargs";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import * as protos from "./protos";
import * as utils from "./utils";

const argv = yargs.option("project-dir", { describe: "Project directory", default: "." }).argv;

var projectDir = argv["project-dir"];

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

var projectConfig = protos.ProjectConfig.create({
  buildPaths: ["models/*"],
  includePaths: ["includes/*"],
});

var projectConfigPath = path.join(projectDir, "dataform.json");

if (fs.existsSync(projectConfigPath)) {
  Object.assign(projectConfig, JSON.parse(fs.readFileSync(projectConfigPath, "utf8")));
}

var packageJsonPath = path.join(projectDir, "package.json");
var packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

var includePaths = glob.sync(projectConfig.includePaths[0], { cwd: projectDir });
var modelPaths = glob.sync(projectConfig.buildPaths[0], { cwd: projectDir });

var packageRequires = Object.keys(packageConfig.dependencies)
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
