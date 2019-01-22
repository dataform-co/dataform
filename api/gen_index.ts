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

  var definitionPaths = [];
  glob.sync("definitions/**/*.{js,sql}", { cwd: projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });
  // Support projects that don't use the new project structure.
  glob.sync("models/**/*.{js,sql}", { cwd: projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });

  var packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `try { global.${utils.variableNameFriendly(packageName)} = require("${packageName}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e.message, "${packageName}");
        } else {
          console.error('Error:', e.message, 'Path: "${packageName}"');
        }
      }`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `try { global.${utils.baseFilename(path)} = require("./${path}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e.message, "${path}");
        } else {
          console.error('Error:', e.message, 'Path: "${path}"');
        }
      }`;
    })
    .join("\n");
  var definitionRequires = definitionPaths
    .map(path => {
      return `try { require("./${path}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e.message, "${path}");
        } else {
          console.error('Error:', e.message, 'Path: "${path}"');
        }
      }`;
    })
    .join("\n");

  return `
    const { init, compile } = require("@dataform/core");
    const protos = require("@dataform/protos");
    ${packageRequires}
    ${includeRequires}
    init("${projectDir}", require("./dataform.json"));
    ${definitionRequires}
    return ${returnOverride || "protos.CompiledGraph.encode(compile()).finish()"};`;
}
