import * as vscode from "vscode";
import { dataform } from "df/protos/ts";
import { workspace } from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind
} from "vscode-languageclient";

let client: LanguageClient;

export async function activate(context: vscode.ExtensionContext) {
  const serverModule = context.asAbsolutePath("server.js");
  const debugOptions = { execArgv: ["--nolazy", "--inspect=6009"] };
  let currentPanel: vscode.WebviewPanel | undefined = undefined;
  let compileGraph: dataform.ICompiledGraph | undefined = undefined;
  let runPromiseResolve: any;
  const statusBarItem = vscode.window.createStatusBarItem("dataform-compile");
  statusBarItem.command = 'dataform.compile';

  function run() {
    vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: "Running...",
      cancellable: true
    }, (progress, token) => {
      token.onCancellationRequested(() => {
        client.sendRequest("cancel");
      });

      const p = new Promise<void>(resolve => {
        runPromiseResolve = resolve;
      });

      return p;
    });
    client.sendRequest("run");
  }

  const serverOptions: ServerOptions = {
    run: { module: serverModule, transport: TransportKind.ipc },
    debug: {
      module: serverModule,
      transport: TransportKind.ipc,
      options: debugOptions
    }
  };

  const clientOptions: LanguageClientOptions = {
    // register server for sqlx files
    documentSelector: [{ scheme: "file", language: "sqlx" }],
    synchronize: {
      fileEvents: workspace.createFileSystemWatcher("**/.clientrc")
    }
  };

  client = new LanguageClient(
    "dataformLanguageServer",
    "Dataform Language Server",
    serverOptions,
    clientOptions
  );

  const compile = vscode.commands.registerCommand("dataform.compile", () => {
    const _ = client.sendRequest("compile");
  });

  const runSubscription = vscode.commands.registerCommand("dataform.run", () => {
    const _ = run();
  });

  const sidebar = vscode.commands.registerCommand('dataform.sidebar', () => {
    // Create and show a new webview
    if (currentPanel) {
      currentPanel.reveal(vscode.ViewColumn.Beside);
      sendCompileDataToWebview(compileGraph, currentPanel);
    } else {
      currentPanel = vscode.window.createWebviewPanel(
        'dataformSidebar', // Identifies the type of the webview. Used internally
        'Dataform', // Title of the panel displayed to the user
        {
          preserveFocus: true, // We don't want the panel to steal focus
          viewColumn: vscode.ViewColumn.Beside // Editor column to show the new webview panel in.
        },
        {
          enableScripts: true
        }
      );
      currentPanel.webview.html = getWebviewContent();
      currentPanel.webview.onDidReceiveMessage(
        message => {
          switch (message.command) {
            case 'run':
              run();
              break;
          }
        },
        undefined,
        context.subscriptions
      );

      sendCompileDataToWebview(compileGraph, currentPanel);

      currentPanel.onDidDispose(() => {
        currentPanel = undefined;
      });
    }
  });

  context.subscriptions.push(compile, runSubscription, sidebar);

  client.start();

  // wait for client to be ready before setting up notification handlers
  await client.onReady();

  client.onNotification("error", errorMessage => {
    vscode.window.showErrorMessage(errorMessage);
  });
  client.onNotification("success", message => {
    vscode.window.showInformationMessage(message);
  });
  client.onNotification("complete", message => {
    runPromiseResolve();
  });

  client.onNotification("compile", (result: dataform.ICompiledGraph) => {
    compileGraph = result;
    if (currentPanel) {
      sendCompileDataToWebview(compileGraph, currentPanel);
    }
  });

  client.onNotification('compile-success', () => {
    statusBarItem.text = '$(pass-filled)';
    statusBarItem.color = 'green';
    statusBarItem.show();
  })

  client.onNotification('compile-fail', () => {
    statusBarItem.text = '$(testing-failed-icon)';
    statusBarItem.color = 'red';
    statusBarItem.show();
  })

  vscode.window.onDidChangeActiveTextEditor((editor: vscode.TextEditor) => {
    if (currentPanel) {
      sendCompileDataToWebview(compileGraph, currentPanel);
    }
  });
}

function sendCompileDataToWebview(compiledGraph: dataform.ICompiledGraph, panel: vscode.WebviewPanel) {
  const activeTextEditor = vscode.window.activeTextEditor;
  if (!activeTextEditor) return;
  const workspaceRootPath = vscode.workspace.rootPath + '/';
  const filePath = vscode.window.activeTextEditor.document.uri.path;
  const currentFileName = filePath.replace(workspaceRootPath, '');

  // EXCLUDE OPERATIONS BECAUSE THEY ARE ANNOYING
  const fileCompile = [
    ...compiledGraph.tables,
    ...compiledGraph.assertions,
  ].find((item) => item.fileName === currentFileName);
  if (!fileCompile) return;
  panel.webview.postMessage({
    command: 'compile',
    name: fileCompile.target?.name,
    tags: fileCompile.tags?.filter((tag) => !!tag).join(', '),
    dependencies: fileCompile.dependencyTargets?.filter((dependency) => !!dependency.name).map((dep) => dep.name).join(', '),
    query: fileCompile?.query,
    errors: compiledGraph?.graphErrors?.compilationErrors?.filter((error) =>
      error.fileName === currentFileName || error.fileName === undefined || error.fileName === '').map((error) => `${error.message},`)
  });
}

function mainScript() {
  const vscode = acquireVsCodeApi();
  const runButton = document.getElementById('run-button');
  runButton.onclick = () => {
    vscode.postMessage({
      command: 'run',
    });
  };
  window.addEventListener('message', event => {
    const message = event.data; // The JSON data our extension sent
    const compiledQueryHeader = document.getElementById('compiled-query-header');
    const compiledQueryTags = document.getElementById('compiled-query-tags');
    const compiledQueryDependencies = document.getElementById('compiled-query-dependencies');
    const query = document.getElementById('compiled-query-box');
    const errors = document.getElementById('error-box');
    switch (message.command) {
      case 'compile':
        compiledQueryHeader.textContent = message.name;
        compiledQueryTags.textContent = message.tags;
        compiledQueryDependencies.textContent = message.dependencies;
        query.textContent = `\n${message.query.trim()}`;
        errors.textContent = message.errors || '';
        break;
    }
  });
}

function getWebviewContent() {
  return `<!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataform!</title>
    <script>
      ${mainScript.toString()}
    </script>
  </head>
  <body>
    <h2>Name: <span id="compiled-query-header"></span></h2>
    <h3>Tags: <span id="compiled-query-tags"></span></h3>
    <h4>Dependencies: <span id="compiled-query-dependencies"></span></h4>
    <code id="error-box" style='margin: 0; display: flex; color: red;'></code>
    <h4>Compiled Query:</h4>
    <pre style='margin: 0;'>
      <code id="compiled-query-box"></code>
    </pre>
    <button id="run-button">Run!</button>
    <script>
      mainScript();
    </script>
  </body>
  </html>`;
}
