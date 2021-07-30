import { decode } from "df/common/protos";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * This is the main entry point into the user space code. This is called by the parent wrapper process with a serialized
 * main config and returns a serialized main result. It replaces the need for index generation and should be called by the wrapper
 * process if it is available on the package instead of using {@see indexFileGenerator}.
 */
export function main(base64EncodedConfig: string): string {
  const globalAny = global as any;

  const config = decode(dataform.GenerateIndexConfig, base64EncodedConfig);

  // Read the project config from the root of the project.
  const originalProjectConfig = require("dataform.json");

  // Stop using the deprecated 'gcloudProjectId' field.
  if (!originalProjectConfig.defaultDatabase) {
    originalProjectConfig.defaultDatabase = originalProjectConfig.gcloudProjectId;
  }
  delete originalProjectConfig.gcloudProjectId;

  let projectConfig = { ...originalProjectConfig };

  // For backwards compatibility, in case core version is ahead of api.
  projectConfig.schemaSuffix =
    config.compileConfig.schemaSuffixOverride || projectConfig.schemaSuffix;

  // Merge in general project config overrides.
  projectConfig = {
    ...projectConfig,
    ...config.compileConfig.projectConfigOverride,
    vars: { ...projectConfig.vars, ...config.compileConfig.projectConfigOverride.vars }
  };

  // Initialize the compilation session.
  const session = require("@dataform/core").session;

  session.init(config.compileConfig.projectDir, projectConfig, originalProjectConfig);

  // Allow "includes" files to use the current session object.
  globalAny.dataform = session;

  // Require "includes" *.js files.

  config.includePaths.forEach(includePath => {
    try {
      // tslint:disable-next-line: tsr-detect-non-literal-require
      globalAny[utils.baseFilename(includePath)] = require(includePath);
    } catch (e) {
      session.compileError(e, includePath);
    }
  });

  // Bind various @dataform/core APIs to the 'global' object.
  globalAny.publish = session.publish.bind(session);
  globalAny.operate = session.operate.bind(session);
  globalAny.assert = session.assert.bind(session);
  globalAny.declare = session.declare.bind(session);
  globalAny.test = session.test.bind(session);

  // Require all "definitions" files (attaching them to the session).
  config.definitionPaths.forEach(definitionPath => {
    try {
      // tslint:disable-next-line: tsr-detect-non-literal-require
      require(definitionPath);
    } catch (e) {
      session.compileError(e, definitionPath);
    }
  });

  // Return a base64 encoded proto via NodeVM. Returning a Uint8Array directly causes issues.
  return session.compileToBase64();
}
