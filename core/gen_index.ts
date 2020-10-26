import { decode } from "df/common/protos";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export function genIndex(base64EncodedConfig: string): string {
  const config = decode(dataform.GenerateIndexConfig, base64EncodedConfig);

  const includeRequires = config.includePaths
    .map(path => {
      return `
      try { global.${utils.baseFilename(path)} = require("./${path}"); } catch (e) {
        session.compileError(e, "${path}");
      }`;
    })
    .join("\n");
  const definitionRequires = config.definitionPaths
    .map(path => {
      return `
      try { require("./${path}"); } catch (e) {
        session.compileError(e, "${path}");
      }`;
    })
    .join("\n");

  const projectOverridesJsonString = JSON.stringify(
    dataform.ProjectConfig.create(config.compileConfig.projectConfigOverride).toJSON()
  );

  const returnValue = !!config.compileConfig.query
    ? `(function() {
      try {
        const ref = global.dataform.resolve.bind(global.dataform);
        const resolve = global.dataform.resolve.bind(global.dataform);
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
const session = require("@dataform/core").session;
session.init("${config.compileConfig.projectDir.replace(
    /\\/g,
    "\\\\"
  )}", projectConfig, originalProjectConfig);

// Allow "includes" files to use the current session object.
global.dataform = session;

// Require "includes" *.js files.
${includeRequires}

// Bind various @dataform/core APIs to the 'global' object.
global.publish = session.publish.bind(session);
global.operate = session.operate.bind(session);
global.assert = session.assert.bind(session);
global.declare = session.declare.bind(session);
global.test = session.test.bind(session);

// Require all "definitions" files (attaching them to the session).
${definitionRequires}

// Return a base64 encoded proto via NodeVM. Returning a Uint8Array directly causes issues.
const base64EncodedGraphBytes = session.compileToBase64();
return ${returnValue};`;
}
