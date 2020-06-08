import { ChildProcess, exec } from "child_process";
import { dataform } from "df/protos/ts";
import {
  createConnection,
  HandlerResult,
  Location,
  ProposedFeatures,
  TextDocuments,
  TextDocumentSyncKind
} from "vscode-languageserver";
import { TextDocument } from "vscode-languageserver-textdocument";

const connection = createConnection(ProposedFeatures.all);
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);
let CACHED_COMPILE_GRAPH: dataform.ICompiledGraph = null;
let WORKSPACE_ROOT_FOLDER: string = null;

connection.onInitialize(() => {
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      // Tell the client that the server supports code completion and definitions
      completionProvider: {
        resolveProvider: true
      },
      definitionProvider: true
    }
  };
});

connection.onInitialized(async () => {
  const _ = compileAndValidate();
  const workSpaceFolders = await connection.workspace.getWorkspaceFolders();
  // the first item is the root folder
  WORKSPACE_ROOT_FOLDER = workSpaceFolders[0].uri;
});

connection.onRequest("compile", async () => {
  const _ = compileAndValidate();
});

documents.onDidSave(change => {
  const _ = compileAndValidate();
});

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

function gatherAllActions(
  graph = CACHED_COMPILE_GRAPH
): Array<dataform.Table | dataform.Declaration | dataform.Operation | dataform.Assertion> {
  return [].concat(graph.tables, graph.operations, graph.assertions, graph.declarations);
}

function retrieveLinkedFileName(ref: string) {
  const allActions = gatherAllActions();
  const foundCompileAction = allActions.find(action => action.name.split(".").slice(-1)[0] === ref);
  return foundCompileAction.fileName;
}

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
    const fileString = `${WORKSPACE_ROOT_FOLDER}/${linkedFileName}`;
    return {
      uri: fileString,
      range: {
        start: { line: 0, character: 0 },
        end: { line: 1, character: 0 }
      }
    } as Location;
  }
);

documents.listen(connection);
connection.listen();
