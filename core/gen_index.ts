import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { util } from "protobufjs";

export function genIndex(base64EncodedConfig: string): string {
  const encodedGraphBytes = new Uint8Array(util.base64.length(base64EncodedConfig));
  util.base64.decode(base64EncodedConfig, encodedGraphBytes, 0);
  const config = dataform.GenerateIndexConfig.decode(encodedGraphBytes);

  const includeRequires = config.includePaths
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
  const definitionRequires = config.definitionPaths
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

  const projectOverridesJsonString = JSON.stringify(
    dataform.ProjectConfig.create(config.compileConfig.projectConfigOverride).toJSON()
  );

  return `
    require("@dataform/core");
    const protos = require("@dataform/protos");
    const { util } = require("protobufjs");
    ${includeRequires}
    let projectConfig = { ...require("./dataform.json") };
    // For backwards compatibility, in case core version is ahead of api.
    projectConfig.schemaSuffix = "${
      config.compileConfig.schemaSuffixOverride
    }" || projectConfig.schemaSuffix;
    // Merge in general project config overrides.
    projectConfig = { ...projectConfig, ...${projectOverridesJsonString} };
    global.session.init("${config.compileConfig.projectDir}", projectConfig);
    ${definitionRequires}
    const compiledGraph = global.session.compile();
    // Keep backwards compatibility with un-namespaced protobufs (i.e. before dataform protobufs were inside a package).
    const protoNamespace = (protos.dataform) ? protos.dataform : protos;
    // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
    const encodedGraphBytes = protoNamespace.CompiledGraph.encode(compiledGraph).finish();
    const base64EncodedGraphBytes = util.base64.encode(encodedGraphBytes, 0, encodedGraphBytes.length);
    return ${config.returnOverride || "base64EncodedGraphBytes"};`;
}
