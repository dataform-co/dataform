import { decode } from "df/common/protos";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export function legacyGenIndex(base64EncodedConfig: string): string {
  const config = decode(dataform.GenerateIndexConfig, base64EncodedConfig);

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

  return `
    const { init, compile } = require("@dataform/core");
    const protos = require("@dataform/protos");
    const { util } = require("protobufjs");
    ${includeRequires}
    const projectConfig = require("./dataform.json");
    projectConfig.schemaSuffix = "${
      config.compileConfig.schemaSuffixOverride
    }" || projectConfig.schemaSuffix;
    init("${config.compileConfig.projectDir}", projectConfig);
    ${definitionRequires}
    const compiledGraph = compile();
    // Keep backwards compatibility with un-namespaced protobufs (i.e. before dataform protobufs were inside a package).
    const protoNamespace = (protos.dataform) ? protos.dataform : protos;
    // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
    const encodedGraphBytes = protoNamespace.CompiledGraph.encode(compiledGraph).finish();
    const base64EncodedGraphBytes = util.base64.encode(encodedGraphBytes, 0, encodedGraphBytes.length);
    return ${config.returnOverride || "base64EncodedGraphBytes"};`;
}
