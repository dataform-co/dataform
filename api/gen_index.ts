import { utils } from "@dataform/core";
import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";

export function genIndex(
  projectDir: string,
  returnOverride?: string,
  defaultSchemaOverride: string = "",
  assertionSchemaOverride: string = ""
): string {
  const packageJsonPath = path.join(projectDir, "package.json");
  const packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  const includePaths = [];
  glob.sync("includes/*.js", { cwd: projectDir }).forEach(path => {
    if (includePaths.indexOf(path) < 0) {
      includePaths.push(path);
    }
  });

  const definitionPaths = [];
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

  const packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `
      try { global.${utils.variableNameFriendly(
        packageName
      )} = require("${packageName}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e, "${packageName}");
        } else {
          console.error('Error:', e.message, 'Path: "${packageName}"');
        }
      }`;
    })
    .join("\n");

  const includeRequires = includePaths
    .map(path => {
      return `
      try { global.${utils.baseFilename(path)} = require("./${path}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e, "${path}");
        } else {
          console.error('Error:', e.message, 'Path: "${path}"');
        }
      }`;
    })
    .join("\n");
  const definitionRequires = definitionPaths
    .map(path => {
      return `
      try { require("./${path}"); } catch (e) {
        if (global.session.compileError) {
          global.session.compileError(e, "${path}");
        } else {
          console.error('Error:', e.message, 'Path: "${path}"');
        }
      }`;
    })
    .join("\n");

  // NOTE: The code in this returned JS calls the locally-installed versions of the @dataform
  // NPM packages, as defined by the project's package.json file. Thus, changes made
  // to code inside e.g. @dataform/core will only take effect on a user's project compilation
  // once that code is pushed to NPM and the user updates their package.json.
  return `
    const { init, compile } = require("@dataform/core");
    const protos = require("@dataform/protos");
    const { util } = require("protobufjs");
    ${packageRequires}
    ${includeRequires}
    const projectConfig = require("./dataform.json");
    projectConfig.defaultSchema = "${defaultSchemaOverride}" || projectConfig.defaultSchema;
    projectConfig.assertionSchema = "${assertionSchemaOverride}" || projectConfig.assertionSchema;
    init("${projectDir}", projectConfig);
    ${definitionRequires}
    const compiledGraph = compile();
    // Keep backwards compatibility with un-namespaced protobufs (i.e. before dataform protobufs were inside a package).
    let protoNamespace = protos.dataform;
    if (!protoNamespace) {
      protoNamespace = protos;
    }
    // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
    const encodedGraphBytes = protoNamespace.CompiledGraph.encode(compiledGraph).finish();
    const base64EncodedGraphBytes = util.base64.encode(encodedGraphBytes, 0, encodedGraphBytes.length);
    return ${returnOverride || "base64EncodedGraphBytes"};`;
}
