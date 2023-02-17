import { decode64 } from "df/common/protos";
import * as utils from "df/core/utils";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export function genIndex(base64EncodedConfig: string): string {
  const config = decode64(dataform.GenerateIndexConfig, base64EncodedConfig);

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
    core.ProjectConfig.create(config.compileConfig.projectConfigOverride).toJSON()
  );

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

// Merge in general project config overrides.
projectConfig = { ...projectConfig, ...${projectOverridesJsonString}, vars: { ...projectConfig.vars, ...${projectOverridesJsonString}.vars } };

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
return session.compileToBase64();`;
}
