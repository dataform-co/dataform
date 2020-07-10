import { util } from "protobufjs";

import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

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

  const returnValue = !!config.compileConfig.query
    ? `(function() {
      try {
        const ref = global.session.resolve.bind(global.session);
        const resolve = global.session.resolve.bind(global.session);
        const self = () => "";
        return \`${config.compileConfig.query}\`;
      } catch (e) {
        return e.message;
      }
    })()`
    : "base64EncodedGraphBytes";

  // NOTE:
  // - The returned script must be valid JavaScript (not TypeScript)
  // - The returned script may not require() any package that is not "@dataform/core"
  return `
// Bind various @dataform/core APIs to the 'global' object.
require("@dataform/core");

// Read the project config.
const originalProjectConfig = require("./dataform.json");

// Stop using the deprecated 'gcloudProjectId' field.
if (!originalProjectConfig.defaultDatabase) {
  originalProjectConfig.defaultDatabase = originalProjectConfig.gcloudProjectId;
}
delete originalProjectConfig.gcloudProjectId;

let projectConfig = { ...originalProjectConfig };

// For backwards compatibility, in case core version is ahead of api.
projectConfig.schemaSuffix = "${
    config.compileConfig.schemaSuffixOverride
  }" || projectConfig.schemaSuffix;

// Merge in general project config overrides.
projectConfig = { ...projectConfig, ...${projectOverridesJsonString} };

// Initialize the compilation session.
global.session.init("${config.compileConfig.projectDir.replace(
    /\\/g,
    "\\\\"
  )}", projectConfig, originalProjectConfig);

// Require "includes" *.js files.
${includeRequires}

// Require all "definitions" files (attaching them to the session).
${definitionRequires}

// Return a base64 encoded proto via NodeVM. Returning a Uint8Array directly causes issues.
const base64EncodedGraphBytes = global.session.compileToBase64();
return ${returnValue};`;
}
