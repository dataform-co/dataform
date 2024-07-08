import { ChildProcess, spawn } from "child_process";
import { ITarget } from "df/core/common";
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
  let compilationFailed = false;
  const spawnedProcess = spawn(
    (process.platform !== "win32") ? "dataform" : "dataform.cmd",
    ["compile", "--json", ...settings.compilerOptions]
  );

  const compileResult = await getProcessResult(spawnedProcess);
  if (compileResult.exitCode !== 0) {
    // tslint:disable-next-line: no-console
    console.error("Error running 'dataform compile':", compileResult);
    if (compileResult.error?.code === "ENOENT") {
      connection.sendNotification(
        "error",
        "Errors encountered when running 'dataform' CLI. Please ensure that the CLI is installed and up-to-date: 'npm i -g @dataform/cli'."
      );
      return;
    } else {
      compilationFailed = true;
    }
  }

  let parsedResult: dataform.ICompiledGraph = null;
  try {
    parsedResult = JSON.parse(compileResult.stdout);
  } catch (e) {
    // tslint:disable-next-line: no-console
    console.error("Error parsing 'dataform compile' output", e);
    connection.sendNotification(
      "error",
      "Error parsing 'dataform compile' output. Please check the output for more information."
    );
    return;
  }

  if (parsedResult?.graphErrors?.compilationErrors) {
    parsedResult.graphErrors.compilationErrors.forEach(compilationError => {
      connection.sendNotification("error", compilationError.fileName + ": " + compilationError.message);
    });
    if (compilationFailed) {
       connection.sendNotification(
         "error",
         "Errors encountered when running 'dataform' CLI. Please check the output for more information."
       );
       return;
    }
  } else {
    connection.sendNotification("success", "Project compiled successfully");
  }
  CACHED_COMPILE_GRAPH = parsedResult;
}

async function getProcessResult(childProcess: ChildProcess) {
  let stdout = "";
  let stderr = "";
  let error: any = null;
  childProcess.stderr.pipe(process.stderr);
  childProcess.stderr.on("data", chunk => (stderr += String(chunk)));
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", chunk => (stdout += String(chunk)));
  childProcess.on("error", err => (error = err));
  const exitCode: number = await new Promise(resolve => {
    childProcess.on("close", resolve);
  });
  return { exitCode, stdout, stderr, error };
}

function gatherAllActions(
  graph = CACHED_COMPILE_GRAPH
): Array<dataform.Table | dataform.Declaration | dataform.Operation | dataform.Assertion> {
  return [].concat(
    graph.tables ?? [],
    graph.operations ?? [],
    graph.assertions ?? [],
    graph.declarations ?? []
  );
}

connection.onDefinition(
  (params): HandlerResult<Location, void> => {
    const currentFile = documents.get(params.textDocument.uri);
    const lineWithRef = currentFile.getText({
      start: { line: params.position.line, character: 0 },
      end: { line: params.position.line + 1, character: 0 }
    });

    const refRegex = new RegExp(/ref\s*\(\s*(["'].+?["'])\s*\)/g); // tslint:disable-line
    const refContents = lineWithRef.match(refRegex);
    if (!refContents || refContents.length === 0) {
      return null;
    }

    // if not compiled yet, we cannot jump to the definition
    if (CACHED_COMPILE_GRAPH === null) {
      connection.sendNotification("info", "Project not compiled yet. Please compile first.");
      return null;
    }

    // Jump to the one that was clicked or closest
    const clickedRef = refContents.map(
      (refContent) => ({
        refContent,
        min: lineWithRef.indexOf(refContent),
        max: lineWithRef.indexOf(refContent) + refContent.length - 1
      })
    ).sort((a, b) => {
      // sort in priority of closest to the clicked position
      // if position is within the refContent, distance is 0
      let distanceToA = 0;
      if (params.position.character < a.min) {
        distanceToA = a.min - params.position.character;
      } else if (params.position.character > a.max) {
        distanceToA = params.position.character - a.max;
      }

      let distanceToB = 0;
      if (params.position.character < b.min) {
        distanceToB = b.min - params.position.character;
      } else if (params.position.character > b.max) {
        distanceToB = params.position.character - b.max;
      }

      return distanceToA - distanceToB;
    })[0].refContent;

    // split to dataset, schema and name
    const linkedTable: ITarget = {database: null, schema: null, name: null};
    const splitMatch = clickedRef.match(/^ref\s*\(\s*(["'](.+?)["'])\s*(,\s*["'](.+?)["']\s*)?(,\s*["'](.+?)["']\s*)?,?\s*\)$/); // tslint:disable-line
    if (splitMatch[6] !== undefined) {
      linkedTable.database = splitMatch[2];
      linkedTable.schema = splitMatch[4];
      linkedTable.name = splitMatch[6];
    } else if (splitMatch[4] !== undefined) {
      linkedTable.schema = splitMatch[2];
      linkedTable.name = splitMatch[4];
    } else if (splitMatch[2] !== undefined) {
      linkedTable.name = splitMatch[2];
    } else {
      return null;
    }

    const foundCompileAction = gatherAllActions().filter(action => (
      (linkedTable.database === null || action?.target?.database !== undefined && action.target.database === linkedTable.database)
      && (linkedTable.schema === null || action?.target?.schema !== undefined && action.target.schema === linkedTable.schema)
      && action?.target?.name !== undefined && action.target.name === linkedTable.name
    ));
    if (foundCompileAction.length === 0) {
      connection.sendNotification("error", `Definition not found for ${clickedRef}`);
      return null;
    } else if (foundCompileAction.length > 1) {
      connection.sendNotification("error", `Multiple definitions found for ${clickedRef}`);
      return null;
    }

    const fileString = `${WORKSPACE_ROOT_FOLDER}/${foundCompileAction[0].fileName}`;
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
