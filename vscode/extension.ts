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
  client.onNotification("error", (errorMessage: string) => {
    vscode.window.showErrorMessage(errorMessage);
  });
  client.onNotification("info", (message: string) => {
    vscode.window.showInformationMessage(message);
  });
  client.onNotification("success", (message: string) => {
    vscode.window.showInformationMessage(message);
  });

  // Recommend YAML extension if not installed
  // We also can add the extension to "extensionDependencies" in package.json,
  // but this way we can avoid forcing users to install the extension.
  // You can control this recommendation behavior through the setting.
  if (workspace.getConfiguration("dataform").get("recommendYamlExtension")) {
    const yamlExtension = vscode.extensions.getExtension("redhat.vscode-yaml");
    if (!yamlExtension) {
      await vscode.window.showInformationMessage(
        "The Dataform extension recommends installing the YAML extension for workflow_settings.yaml support.",
        "Install",
        "Don't show again"
      ).then(selection => {
        if (selection === "Install") {
          // Open the YAML extension page
          vscode.env.openExternal(
            vscode.Uri.parse(
              "vscode:extension/redhat.vscode-yaml"
            )
          );
        } else if (selection === "Don't show again") {
          // Disable the recommendation
          workspace.getConfiguration("dataform").update(
            "recommendYamlExtension",
            false,
            vscode.ConfigurationTarget.Global
          );
        }
      });
    }
  }
}
