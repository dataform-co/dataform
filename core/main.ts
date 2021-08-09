import { decode64, encode64 } from "df/common/protos";
import { Session } from "df/core/session";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * This is the main entry point into the user space code that should be invoked by the compilation wrapper sandbox.
 *
 * @param encodedMainConfig a base64 encoded {@see dataform.MainConfig} proto.
 * @returns a base64 encoded {@see dataform.MainResult} proto.
 */
export function main(encodedMainConfig: string): string {
  const globalAny = global as any;

  const config = decode64(dataform.MainConfig, encodedMainConfig);

  // Read the project config from the root of the project.
  const originalProjectConfig = require("dataform.json");

  const projectConfigOverride = config.compileConfig.projectConfigOverride ?? {};

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
    ...projectConfigOverride,
    vars: { ...projectConfig.vars, ...projectConfigOverride.vars }
  };

  // Initialize the compilation session.
  const session = require("@dataform/core").session as Session;

  session.init(config.compileConfig.projectDir, projectConfig, originalProjectConfig);

  // Allow "includes" files to use the current session object.
  globalAny.dataform = session;

  // Require "includes" *.js files.
  config.compileConfig.filePaths
    .filter(path => path.startsWith("includes/"))
    .filter(path => path.endsWith(".js"))
    .forEach(includePath => {
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
  config.compileConfig.filePaths
    .filter(path => path.startsWith("definitions/"))
    .filter(path => path.endsWith(".js") || path.endsWith(".sqlx"))
    .forEach(definitionPath => {
      try {
        // tslint:disable-next-line: tsr-detect-non-literal-require
        require(definitionPath);
      } catch (e) {
        session.compileError(e, definitionPath);
      }
    });

  // Return a base64 encoded proto. Returning a Uint8Array directly causes issues.
  return encode64(
    dataform.MainResult,
    dataform.MainResult.create({ compiledGraph: session.compile() })
  );
}
