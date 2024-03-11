import { decode64, encode64, verifyObjectMatchesProto } from "df/common/protos";
import { Assertion } from "df/core/actions/assertion";
import { Declaration } from "df/core/actions/declaration";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import { nativeRequire } from "df/core/utils";
import { readWorkflowSettings } from "df/core/workflow_settings";
import { dataform } from "df/protos/ts";

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

  // Read the workflow settings from the root of the project.
  let projectConfig = readWorkflowSettings();

  // Merge in project config overrides.
  const projectConfigOverride = compileRequest.compileConfig.projectConfigOverride ?? {};
  projectConfig = dataform.ProjectConfig.create({
    ...projectConfig,
    ...projectConfigOverride,
    vars: { ...projectConfig.vars, ...projectConfigOverride.vars }
  });

  // Initialize the compilation session.
  const session = nativeRequire("@dataform/core").session as Session;
  session.init(compileRequest.compileConfig.projectDir, projectConfig, projectConfig);

  // Allow "includes" files to use the current session object.
  globalAny.dataform = session;

  // Require "includes/*.js" files, attaching them (by file basename) to the `global` object.
  // We delay attaching them to `global` until after all have been required, to prevent
  // "includes" files from implicitly depending on other "includes" files.
  const topLevelIncludes: { [key: string]: any } = {};
  compileRequest.compileConfig.filePaths
    .filter(path => path.startsWith(`includes${Path.separator}`))
    .filter(path => path.split(Path.separator).length === 2) // Only include top-level "includes" files.
    .filter(path => Path.fileExtension(path) === "js")
    .forEach(includePath => {
      try {
        // tslint:disable-next-line: tsr-detect-non-literal-require
        topLevelIncludes[Path.fileName(includePath)] = nativeRequire(includePath);
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
  globalAny.notebook = session.notebook.bind(session);
  globalAny.test = session.test.bind(session);

  loadActionConfigs(session, compileRequest.compileConfig.filePaths);

  // Require all "definitions" files (attaching them to the session).
  compileRequest.compileConfig.filePaths
    .filter(path => path.startsWith(`definitions${Path.separator}`))
    .filter(path => Path.fileExtension(path) === "js" || Path.fileExtension(path) === "sqlx")
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
      path =>
        path.startsWith(`definitions${Path.separator}`) &&
        Path.fileName(path) === "actions" &&
        Path.fileExtension(path) === "yaml"
    )
    .sort()
    .forEach(actionConfigsPath => {
      const actionConfigs = loadActionConfigsFile(session, actionConfigsPath);
      actionConfigs.actions.forEach(nonProtoActionConfig => {
        const actionConfig = dataform.ActionConfig.create(nonProtoActionConfig);

        if (actionConfig.table) {
          session.actions.push(
            new Table(
              session,
              dataform.ActionConfig.TableConfig.create(actionConfig.table),
              "table",
              actionConfigsPath
            )
          );
        } else if (actionConfig.view) {
          session.actions.push(
            new Table(
              session,
              dataform.ActionConfig.ViewConfig.create(actionConfig.view),
              "view",
              actionConfigsPath
            )
          );
        } else if (actionConfig.incrementalTable) {
          session.actions.push(
            new Table(
              session,
              dataform.ActionConfig.IncrementalTableConfig.create(actionConfig.incrementalTable),
              "incremental",
              actionConfigsPath
            )
          );
        } else if (actionConfig.assertion) {
          session.actions.push(
            new Assertion(
              session,
              dataform.ActionConfig.AssertionConfig.create(actionConfig.assertion),
              actionConfigsPath
            )
          );
        } else if (actionConfig.operation) {
          session.actions.push(
            new Operation(
              session,
              dataform.ActionConfig.OperationConfig.create(actionConfig.operation),
              actionConfigsPath
            )
          );
        } else if (actionConfig.declaration) {
          session.actions.push(
            new Declaration(
              session,
              dataform.ActionConfig.DeclarationConfig.create(actionConfig.declaration)
            )
          );
        } else if (actionConfig.notebook) {
          session.actions.push(
            new Notebook(
              session,
              dataform.ActionConfig.NotebookConfig.create(actionConfig.notebook),
              actionConfigsPath
            )
          );
        } else {
          throw Error("Empty action configs are not permitted.");
        }
      });
    });
}

function loadActionConfigsFile(
  session: Session,
  actionConfigsPath: string
): dataform.ActionConfigs {
  let actionConfigsAsJson = {};
  try {
    // tslint:disable-next-line: tsr-detect-non-literal-require
    actionConfigsAsJson = nativeRequire(actionConfigsPath).asJson;
  } catch (e) {
    session.compileError(e, actionConfigsPath);
  }
  verifyObjectMatchesProto(dataform.ActionConfigs, actionConfigsAsJson);
  return dataform.ActionConfigs.fromObject(actionConfigsAsJson);
}
