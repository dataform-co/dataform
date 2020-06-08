/* --------------------------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 * ------------------------------------------------------------------------------------------ */
import { ChildProcess, exec } from "child_process";
import { dataform } from "df/protos/ts";
import {
  createConnection,
  DidChangeConfigurationNotification,
  HandlerResult,
  InitializeParams,
  InitializeResult,
  Location,
  ProposedFeatures,
  TextDocuments,
  TextDocumentSyncKind
} from "vscode-languageserver";
import { TextDocument } from "vscode-languageserver-textdocument";

const DIRECTORY_OF_TEST_REPO = "/Users/georgemcgowan/Work/clones/dataform-data/";

const types = ["dataset", "assertion", "operation", "declaration"] as const;
export type Type = typeof types[number];
interface IAction {
  name: string;
  type: Type;
  fileName: string;
  dependencies?: string[];
  target?: dataform.ITarget;
}

// Create a connection for the server. The connection uses Node's IPC as a transport.
// Also include all preview / proposed LSP features.
const connection = createConnection(ProposedFeatures.all);
// Create a simple text document manager. The text document manager
// supports full document sync only
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
let hasConfigurationCapability: boolean = false;
let hasWorkspaceFolderCapability: boolean = false;
let hasDiagnosticRelatedInformationCapability: boolean = false;
connection.onInitialize((params: InitializeParams) => {
  const capabilities = params.capabilities;
  // Does the client support the `workspace/configuration` request?
  // If not, we will fall back using global settings
  hasConfigurationCapability = !!(capabilities.workspace && !!capabilities.workspace.configuration);
  hasWorkspaceFolderCapability = !!(
    capabilities.workspace && !!capabilities.workspace.workspaceFolders
  );
  hasDiagnosticRelatedInformationCapability = !!(
    capabilities.textDocument &&
    capabilities.textDocument.publishDiagnostics &&
    capabilities.textDocument.publishDiagnostics.relatedInformation
  );
  const result: InitializeResult = {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      // Tell the client that the server supports code completion
      completionProvider: {
        resolveProvider: true
      },
      definitionProvider: true
    }
  };
  if (hasWorkspaceFolderCapability) {
    result.capabilities.workspace = {
      workspaceFolders: {
        supported: true
      }
    };
  }
  return result;
});
connection.onInitialized(() => {
  if (hasConfigurationCapability) {
    // Register for all configuration changes.
    connection.client.register(DidChangeConfigurationNotification.type, undefined);
  }
  if (hasWorkspaceFolderCapability) {
    connection.workspace.onDidChangeWorkspaceFolders(_event => {
      connection.console.log("Workspace folder change event received.");
    });
  }
  const _ = compileAndValidate();
});

// The example settings
interface ExampleSettings {
  maxNumberOfProblems: number;
}
// The global settings, used when the `workspace/configuration` request is not supported by the client.
// Please note that this is not the case when using this server with the client provided in this example
// but could happen with other clients.
const defaultSettings: ExampleSettings = { maxNumberOfProblems: 1000 };
let globalSettings: ExampleSettings = defaultSettings;
// Cache the settings of all open documents
const documentSettings: Map<string, Thenable<ExampleSettings>> = new Map();
connection.onDidChangeConfiguration(change => {
  if (hasConfigurationCapability) {
    // Reset all cached document settings
    documentSettings.clear();
  } else {
    globalSettings = (change.settings.languageServerExample || defaultSettings) as ExampleSettings;
  }
});
// Only keep settings for open documents
documents.onDidClose(e => {
  documentSettings.delete(e.document.uri);
});

// doesn't work at the moment
connection.onRequest("run", async () => {
  connection.sendNotification("success", "Trying to run!");
  try {
    const dryRun = await getProcessResult(exec("dataform run --dry-run --json"));
    console.log("exit code", dryRun.exitCode);
    const parsedDryRunResult = await JSON.parse(dryRun.stdout);
    connection.sendNotification("success", "Successful dry run!");
  } catch (e) {
    console.log(e);
    connection.sendNotification("error", e);
  }
});

connection.onRequest("compile", async () => {
  const _ = compileAndValidate();
});

// The content of a text document has changed. This event is emitted
// when the text document first opened or when its content has changed.
documents.onDidSave(change => {
  const _ = compileAndValidate();
});

let CACHED_COMPILE_GRAPH: dataform.ICompiledGraph = null;

async function compileAndValidate() {
  const compileResult = await getProcessResult(exec("dataform compile --json"));
  const parsedResult: dataform.ICompiledGraph = JSON.parse(compileResult.stdout);
  if (parsedResult?.graphErrors?.compilationErrors) {
    parsedResult.graphErrors.compilationErrors.forEach(compilationError => {
      connection.sendNotification("error", compilationError.message);
    });
  } else {
    connection.sendNotification("success", "Project compiled successfully");
  }
  CACHED_COMPILE_GRAPH = parsedResult;
}

async function getProcessResult(childProcess: ChildProcess) {
  let stdout = "";
  childProcess.stderr.pipe(process.stderr);
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", chunk => (stdout += String(chunk)));
  const exitCode: number = await new Promise(resolve => {
    childProcess.on("close", resolve);
  });
  return { exitCode, stdout };
}

function convertGraphToActions(graph = CACHED_COMPILE_GRAPH) {
  return [].concat(
    (graph.tables || []).map(t => ({ ...t, type: "dataset" })),
    (graph.operations || []).map(o => ({ ...o, type: "operation" })),
    (graph.assertions || []).map(o => ({ ...o, type: "assertion" })),
    (graph.declarations || []).map(o => ({ ...o, type: "declaration" }))
  ) as IAction[];
}

function retrieveLinkedFileName(ref: string) {
  const actionGraph = convertGraphToActions();
  const foundCompileAction = actionGraph.find(
    action => action.name.split(".").slice(-1)[0] === ref
  );
  return foundCompileAction.fileName;
}

// this may be the right way to do things...
connection.onDefinition(
  (params): HandlerResult<Location, void> => {
    const currentFile = documents.get(params.textDocument.uri);
    const lineWithRef = currentFile.getText({
      start: { line: params.position.line, character: 0 },
      end: { line: params.position.line + 1, character: 0 }
    });
    const refRegex = new RegExp(/(?<=ref\(\"|'\s*).*?(?=\s*\"|'\))/g);
    const refContents = lineWithRef.match(refRegex);
    if (!refContents || refContents.length === 0) {
      return null;
    }
    const linkedFileName = retrieveLinkedFileName(refContents.join(""));
    // TODO: Make this not george specific
    const fileString = `${DIRECTORY_OF_TEST_REPO}${linkedFileName}`;
    return {
      uri: `file://${fileString}`,
      // just go to the top of the file and select nothing
      range: {
        start: { line: 0, character: 0 },
        end: { line: 1, character: 0 }
      }
    } as Location;
  }
);

// Make the text document manager listen on the connection
// for open, change and close document events
documents.listen(connection);
// Listen on the connection
connection.listen();
