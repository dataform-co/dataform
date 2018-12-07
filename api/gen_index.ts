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
      return `try { global.${utils.variableNameFriendly(
        packageName
      )} = require("${packageName}"); } catch (e) { throw Error("Exception in ${packageName}: " + e); }`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `try { global.${utils.baseFilename(
        path
      )} = require("./${path}"); } catch (e) { throw Error("Exception in ${path}: " + e); }`;
    })
    .join("\n");
  var datasetRequires = datasetPaths
    .map(path => {
      return `try { require("./${path}"); } catch (e) { throw Error("Exception in ${path}: " + e); }`;
    })
    .join("\n");

  return `
    const { init, compile } = require("@dataform/core");
    ${packageRequires}
    ${includeRequires}
    init("${projectDir}", require("./dataform.json"));
    ${datasetRequires}
    return ${returnOverride || "compile()"};`;
}
