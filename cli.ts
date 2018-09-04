#!/usr/bin/env node
import * as fs from "fs";
import * as util from "util";
import * as yargs from "yargs";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import * as protos from "./protos";

const argv = yargs.option("project-dir", { describe: "Project directory", default: "." }).argv;

var projectDir = argv["project-dir"];

const vm = new NodeVM({
  timeout: 5000,
  wrapper: "none",
  require: {
    context: "sandbox",
    root: "./",
    external: true
  }
});

var projectConfig = protos.ProjectConfig.create({
  modelPaths: ["models/*"],
  includePaths: ["includes/*"]
});

var projectConfigPath = path.join(projectDir, "dataform.json");

if (fs.existsSync(projectConfigPath)) {
  Object.assign(projectConfig, JSON.parse(fs.readFileSync(projectConfigPath, "utf8")));
}

var includePaths = glob.sync(projectConfig.includePaths[0], { cwd: projectDir });
var modelPaths = glob.sync(projectConfig.modelPaths[0], { cwd: projectDir });

console.log(includePaths);
console.log(modelPaths);

var includeRequires = includePaths
  .map(path => {
    return `require("./${path}");`;
  })
  .join("\n");
var modelRequires = modelPaths
  .map(path => {
    return `require("./${path}");`;
  })
  .join("\n");

var mainScript = `
const dft = require("dft");
global.materialize = dft.materialize;
global.operation = dft.operation;
global.assertion = dft.assertion;
${includeRequires}
${modelRequires}
return dft.build();`;

var output = vm.run(mainScript, path.resolve(path.join(projectDir, "index.js")));

console.log(JSON.stringify(output, null, 4));
