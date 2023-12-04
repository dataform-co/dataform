import { decode64, encode64, verifyObjectMatchesProto } from "df/common/protos";
import { Session } from "df/core/session";
import * as utils from "df/core/utils";
import { readWorkflowSettings } from "df/core/workflow_settings";
import { dataform } from "df/protos/ts";

declare var __webpack_require__: any;
declare var __non_webpack_require__: any;

// If this code is bundled with webpack, we need to side-step the webpack require re-writing and use the real require method in here.
const nativeRequire = typeof __webpack_require__ === "function" ? __non_webpack_require__ : require;

/**
 * This is the main entry point into the user space code that should be invoked by the compilation wrapper sandbox.
 *
 * @param coreExecutionRequest an encoded {@see dataform.CoreExecutionRequest} proto.
 * @returns an encoded {@see dataform.CoreExecutionResponse} proto.
 */
export function main(coreExecutionRequest: Uint8Array | string): Uint8Array | string {
  const globalAny = global as any;

  let request: dataform.CoreExecutionRequest;
  if (typeof coreExecutionRequest === "string") {
    // Older versions of the Dataform CLI send a base64 encoded string.
    // See https://github.com/dataform-co/dataform/pull/1570.
    request = decode64(dataform.CoreExecutionRequest, coreExecutionRequest);
  } else {
    request = dataform.CoreExecutionRequest.decode(coreExecutionRequest);
  }
  const compileRequest = request.compile;

  // Read the project config from the root of the project.
  const originalProjectConfig = readWorkflowSettings();

  const projectConfigOverride = compileRequest.compileConfig.projectConfigOverride ?? {};

  let projectConfig = { ...originalProjectConfig };

  // Merge in general project config overrides.
  projectConfig = {
    ...projectConfig,
    ...projectConfigOverride,
    vars: { ...projectConfig.vars, ...projectConfigOverride.vars }
  };

  // Initialize the compilation session.
  const session = nativeRequire("@dataform/core").session as Session;

  session.init(compileRequest.compileConfig.projectDir, projectConfig, originalProjectConfig);

  // Allow "includes" files to use the current session object.
  globalAny.dataform = session;

  // Require "includes/*.js" files, attaching them (by file basename) to the `global` object.
  // We delay attaching them to `global` until after all have been required, to prevent
  // "includes" files from implicitly depending on other "includes" files.
  const topLevelIncludes: { [key: string]: any } = {};
  compileRequest.compileConfig.filePaths
    .filter(path => path.startsWith(`includes${utils.pathSeperator}`))
    .filter(path => path.split(utils.pathSeperator).length === 2) // Only include top-level "includes" files.
    .filter(path => path.endsWith(".js"))
    .forEach(includePath => {
      try {
        // tslint:disable-next-line: tsr-detect-non-literal-require
        topLevelIncludes[utils.baseFilename(includePath)] = nativeRequire(includePath);
      } catch (e) {
        session.compileError(e, includePath);
      }
    });
  Object.assign(globalAny, topLevelIncludes);

  // Bind various @dataform/core APIs to the 'global' object.
  globalAny.publish = session.publish.bind(session);
  globalAny.operate = session.operate.bind(session);
  globalAny.assert = session.assert.bind(session);
  globalAny.declare = session.declare.bind(session);
  globalAny.test = session.test.bind(session);

  loadActionConfigs(session, compileRequest.compileConfig.filePaths);

  // Require all "definitions" files (attaching them to the session).
  compileRequest.compileConfig.filePaths
    .filter(path => path.startsWith(`definitions${utils.pathSeperator}`))
    .filter(path => path.endsWith(".js") || path.endsWith(".sqlx"))
    .sort()
    .forEach(definitionPath => {
      try {
        // tslint:disable-next-line: tsr-detect-non-literal-require
        nativeRequire(definitionPath);
      } catch (e) {
        session.compileError(e, definitionPath);
      }
    });

  const coreExecutionResponse = dataform.CoreExecutionResponse.create({
    compile: { compiledGraph: session.compile() }
  });

  if (typeof coreExecutionRequest === "string") {
    // Older versions of the Dataform CLI expect a base64 encoded string to be returned.
    // See https://github.com/dataform-co/dataform/pull/1570.
    return encode64(dataform.CoreExecutionResponse, coreExecutionResponse);
  }

  return dataform.CoreExecutionResponse.encode(coreExecutionResponse).finish();
}

function loadActionConfigs(session: Session, filePaths: string[]) {
  filePaths
    .filter(
      path => path.startsWith(`definitions${utils.pathSeperator}`) && path.endsWith("actions.yaml")
    )
    .sort()
    .forEach(actionConfigsPath => {
      let actionConfigsAsJson = {};
      try {
        // tslint:disable-next-line: tsr-detect-non-literal-require
        actionConfigsAsJson = nativeRequire(actionConfigsPath).asJson();
      } catch (e) {
        session.compileError(e, actionConfigsPath);
      }
      // TODO(ekrekr): Throw nice errors if the proto is invalid.
      verifyObjectMatchesProto(dataform.ActionConfigs, actionConfigsAsJson);
      const actionConfigs = dataform.ActionConfigs.fromObject(actionConfigsAsJson);

      actionConfigs.actions.forEach(actionConfig => {
        const { fileExtension, fileNameAsTargetName } = extractActionDetailsFromFileName(
          actionConfig.fileName
        );
        if (!actionConfig.target?.name) {
          if (!actionConfig.target) {
            actionConfig.target = {};
          }
          actionConfig.target.name = fileNameAsTargetName;
        }

        if (fileExtension === "ipynb") {
          // TODO(ekrekr): throw an error if any non-notebook configs are given.
          // TODO(ekrekr): add test for nice errors if file not found.
          const notebookContents = nativeRequire(actionConfig.fileName).asBase64String();
          session.notebookAction(dataform.ActionConfig.create(actionConfig), notebookContents);
        }
      });
    });
}

function extractActionDetailsFromFileName(
  path: string
): { fileExtension: string; fileNameAsTargetName: string } {
  // TODO(ekrekr): make filename in actions.yaml files not need the `definitions` prefix. In
  // addition, actions.yaml in nested directories should prefix file imports with their path.
  const fileName = path.split("/").slice(-1)[0];
  const fileExtension = fileName.split(".").slice(-1)[0];
  // TODO(ekrekr): validate and test weird characters in filenames.
  const fileNameAsTargetName = fileName.slice(0, fileExtension.length - 1);
  return { fileExtension, fileNameAsTargetName };
}
