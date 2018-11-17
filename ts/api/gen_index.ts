import * as fs from "fs";
import * as path from "path";
import * as glob from "glob";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";

export function genIndex(projectDir: string, returnOverride?: string): string {
  var projectConfig = protos.ProjectConfig.create({
    defaultSchema: "dataform",
    assertionSchema: "dataform_assertions"
  });

  var projectConfigPath = path.join(projectDir, "dataform.json");
  if (fs.existsSync(projectConfigPath)) {
    Object.assign(projectConfig, JSON.parse(fs.readFileSync(projectConfigPath, "utf8")));
  }

  var packageJsonPath = path.join(projectDir, "package.json");
  var packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  var includePaths = [];
  glob.sync("includes/*.js", { cwd: projectDir }).forEach(path => {
    if (includePaths.indexOf(path) < 0) {
      includePaths.push(path);
    }
  });

  var datasetPaths = [];
  glob.sync("models/**/*.{js,sql}", { cwd: projectDir }).forEach(path => {
    if (datasetPaths.indexOf(path) < 0) {
      datasetPaths.push(path);
    }
  });

  var packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `global.${utils.variableNameFriendly(packageName)} = require("${packageName}");`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `try { global.${utils.baseFilename(
        path
      )} = require("./${path}"); } catch (e) { throw Error("Exception in ${path}: " + e) }`;
    })
    .join("\n");
  var datasetRequires = datasetPaths
    .map(path => {
      return `try { require("./${path}"); } catch (e) { throw Error("Exception in ${path}: " + e) }`;
    })
    .join("\n");

  return `
    console.log("DEBUG(fork): gen_index_start:\t " + Date.now());
    const dataformcore = require("@dataform/core");
    dataformcore.Dataform.ROOT_DIR="${projectDir}";
    dataformcore.init(require("./dataform.json"));
    console.log("DEBUG(fork): post_init:\t " + Date.now());
    ${packageRequires}
    console.log("DEBUG(fork): post_pkg_requires:\t " + Date.now());
    ${includeRequires}
    console.log("DEBUG(fork): post_include_requires:\t " + Date.now());
    ${datasetRequires}
    console.log("DEBUG(fork): post_dataset_requires:\t " + Date.now());
    var returnValue = ${returnOverride || "dataformcore.compile()"};
    console.log("DEBUG(fork): post_return_value:\t " + Date.now());
    return returnValue;
    `;
}
