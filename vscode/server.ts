import { ChildProcess, spawn } from "child_process";
import { dataform } from "df/protos/ts";
import {
  createConnection,
  DidChangeConfigurationNotification,
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

let settings = {
  compilerOptions: [] as string[],
  compileOnSave: true
};

connection.onInitialize(() => {
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      // Tell the client that the server supports definitions
      definitionProvider: true
    }
  };
});

connection.onInitialized(async () => {
  await Promise.all([applySettings(), connection.client.register(DidChangeConfigurationNotification.type)]);
  const _ = compileAndValidate();
  const workSpaceFolders = await connection.workspace.getWorkspaceFolders();
  // the first item is the root folder
  WORKSPACE_ROOT_FOLDER = workSpaceFolders[0].uri;
});

connection.onDidChangeConfiguration(async () => {
  await applySettings();
  await compileAndValidate();
})

connection.onRequest("compile", async () => {
  const _ = compileAndValidate();
});

documents.onDidSave(change => {
  if (settings.compileOnSave) {
    const _ = compileAndValidate();
  }
});

async function applySettings() {
  settings = await connection.workspace.getConfiguration('dataform');
}

async function compileAndValidate() {
  const spawnedProcess = spawn("dataform", ["compile", "--json", ...settings.compilerOptions]);
  spawnedProcess.on("error", err => {
    // tslint:disable-next-line: no-console
    console.error("Error running 'dataform compile':", err);
    connection.sendNotification(
      "error",
      "Errors encountered when running 'dataform' CLI. Please ensure that the CLI is installed and up-to-date: 'npm i -g @dataform/cli'."
    );
  });
  const compileResult = await getProcessResult(spawnedProcess);

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
  const foundCompileAction = allActions.find(action => action.target.name === ref);
  return foundCompileAction.fileName;
}

connection.onDefinition(
  (params): HandlerResult<Location, void> => {
    const currentFile = documents.get(params.textDocument.uri);
    const lineWithRef = currentFile.getText({
      start: { line: params.position.line, character: 0 },
      end: { line: params.position.line + 1, character: 0 }
    });

    const refRegex = new RegExp(/(?<=ref\(\"|'\s*).*?(?=\s*\"|'\))/g); // tslint:disable-line
    const refContents = lineWithRef.match(refRegex);
    if (!refContents || refContents.length === 0) {
      return null;
    }

    const minPosition = lineWithRef.search(refRegex);
    const refStatement = refContents[0];
    const maxPosition = minPosition + refStatement.length;

    if (params.position.character > minPosition && params.position.character < maxPosition) {
      // TODO: Make this work for multiple refs in one line
      const linkedFileName = retrieveLinkedFileName(refContents[0]);
      const fileString = `${WORKSPACE_ROOT_FOLDER}/${linkedFileName}`;
      return {
        uri: fileString,
        range: {
          start: { line: 0, character: 0 },
          end: { line: 1, character: 0 }
        }
      } as Location;
    }
  }
);

documents.listen(connection);
connection.listen();
