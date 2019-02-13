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
          global.session.compileError(e, "${packageName}");
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
          global.session.compileError(e, "${path}");
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
          global.session.compileError(e, "${path}");
        } else {
          console.error('Error:', e.message, 'Path: "${path}"');
        }
      }`;
    })
    .join("\n");

  return `
    const { init, compile } = require("@dataform/core");
    const protos = require("@dataform/protos");
    const { util } = require("protobufjs");
    ${packageRequires}
    ${includeRequires}
    init("${projectDir}", require("./dataform.json"));
    ${definitionRequires}
    const compiledGraph = compile();
    // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
    const encodedGraphBytes = protos.CompiledGraph.encode(compiledGraph).finish();
    const base64EncodedGraphBytes = util.base64.encode(encodedGraphBytes, 0, encodedGraphBytes.length);
    return ${returnOverride || "base64EncodedGraphBytes"};`;
}
