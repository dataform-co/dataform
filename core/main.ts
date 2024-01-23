import { decode64, encode64, verifyObjectMatchesProto } from "df/common/protos";
import { Assertion } from "df/core/actions/assertion";
import { Declaration } from "df/core/actions/declaration";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import * as Path from "df/core/path";
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

  // Read the workflow settings from the root of the project.
  const workflowSettings = readWorkflowSettings();

  // Convert the workflow settings to the compiled graph's project config structure.
  let projectConfig = dataform.ProjectConfig.create({
    warehouse: "bigquery",
    defaultDatabase: workflowSettings.defaultProject,
    defaultSchema: workflowSettings.defaultDataset,
    defaultLocation: workflowSettings.defaultLocation,
    assertionSchema: workflowSettings.defaultAssertionDataset,
    vars: workflowSettings.vars,
    databaseSuffix: workflowSettings.projectSuffix,
    schemaSuffix: workflowSettings.datasetSuffix,
    tablePrefix: workflowSettings.actionPrefix
  });

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
    .filter(path => path.endsWith(".js"))
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
  globalAny.test = session.test.bind(session);

  loadActionConfigs(session, compileRequest.compileConfig.filePaths);

  // Require all "definitions" files (attaching them to the session).
  compileRequest.compileConfig.filePaths
    .filter(path => path.startsWith(`definitions${Path.separator}`))
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
      path => path.startsWith(`definitions${Path.separator}`) && path.endsWith("/actions.yaml")
    )
    .sort()
    .forEach(actionConfigsPath => {
      const actionConfigs = loadActionConfigsFile(session, actionConfigsPath);
      actionConfigs.actions.forEach(nonProtoActionConfig => {
        const actionConfig = dataform.ActionConfig.create(nonProtoActionConfig);

        if (actionConfig.declaration) {
          if (actionConfig.fileName) {
            throw Error(
              "Declaration configs cannot have 'fileName' fields as they cannot take source files"
            );
          }
          if (!actionConfig.target?.name) {
            throw Error(
              "Declaration configs must include a 'target' with a populated 'name' field"
            );
          }

          const declaration = new Declaration(session, actionConfig);
          session.actions.push(declaration);
          return;
        }

        const { fileExtension, fileNameAsTargetName } = utils.extractActionDetailsFromFileName(
          actionConfig.fileName
        );
        if (!actionConfig.target) {
          actionConfig.target = {};
        }
        if (!actionConfig.target.name) {
          actionConfig.target.name = fileNameAsTargetName;
        }

        // Users define file paths relative to action config path, but internally and in the
        // compiled graph they are absolute paths.
        actionConfig.fileName =
          actionConfigsPath.substring(0, actionConfigsPath.length - "actions.yaml".length) +
          actionConfig.fileName;

        if (fileExtension === "ipynb") {
          const notebookContents = nativeRequire(actionConfig.fileName).asJson;
          const strippedNotebookContents = stripNotebookOutputs(
            notebookContents,
            actionConfig.fileName
          );
          const notebook = new Notebook(session, actionConfig);
          notebook.ipynb(strippedNotebookContents);
          session.actions.push(notebook);
        }

        if (fileExtension === "sql") {
          loadSqlFile(session, actionConfig);
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

function loadSqlFile(session: Session, actionConfig: dataform.ActionConfig) {
  const queryAsContextable = nativeRequire(actionConfig.fileName).queryAsContextable;

  if (actionConfig.assertion) {
    const assertion = new Assertion(session, actionConfig);
    assertion.query(queryAsContextable);
    session.actions.push(assertion);
    return;
  }
  if (actionConfig.table || actionConfig.incrementalTable || actionConfig.view) {
    const table = new Table(session, actionConfig);
    table.query(queryAsContextable);
    session.actions.push(table);
    return;
  }
  // If no config is specified, the operation action type is defaulted to.
  const operation = new Operation(session, actionConfig);
  operation.queries(queryAsContextable);
  session.actions.push(operation);
}

function stripNotebookOutputs(
  notebookAsJson: { [key: string]: unknown },
  path: string
): { [key: string]: unknown } {
  if (!("cells" in notebookAsJson)) {
    throw new Error(`Notebook at ${path} is invalid: cells field not present`);
  }
  (notebookAsJson.cells as Array<{ [key: string]: unknown }>).forEach((cell, index) => {
    if ("outputs" in cell) {
      cell.outputs = [];
      (notebookAsJson.cells as Array<{ [key: string]: unknown }>)[index] = cell;
    }
  });
  return notebookAsJson;
}
