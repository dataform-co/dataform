import * as vscode from "vscode";
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

  context.subscriptions.push(compile);

  client.start();

  // wait for client to be ready before setting up notification handlers
  await client.onReady();
  client.onNotification("error", errorMessage => {
    vscode.window.showErrorMessage(errorMessage);
  });
  client.onNotification("success", message => {
    vscode.window.showInformationMessage(message);
  });
}
