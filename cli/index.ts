import yargs from "yargs";

import {
  compileCommand,
  formatCommand,
  helpCommand,
  initCommand,
  initCredsCommand,
  installCommand,
  runCommand,
  testCommand
} from "df/cli/commands";
import { printError } from "df/cli/console";
import { createYargsCli } from "df/cli/yargswrapper";

process.on("unhandledRejection", async (reason: any) => {
  printError(`Unhandled promise rejection: ${reason?.stack || reason}`);
});

// TODO: Since yargs launched an actually well typed API in version 12, let's use it as this file is currently not type checked.
export function runCli() {
  const builtYargs = createYargsCli({
    commands: [
      helpCommand,
      initCommand,
      installCommand,
      initCredsCommand,
      compileCommand,
      testCommand,
      runCommand,
      formatCommand
    ]
  })
    .scriptName("dataform")
    .strict()
    .wrap(null)
    .recommendCommands()
    .fail(async (msg: string, err: any) => {
      if (!!err && err.name === "VMError" && err.message.includes("Cannot find module")) {
        printError("Could not find NPM dependencies. Have you run 'dataform install'?");
      } else {
        const message = err?.message ? err.message.split("\n")[0] : msg;
        printError(`Dataform encountered an error: ${message}`);
        if (err?.stack) {
          printError(err.stack);
        }
      }
      process.exit(1);
    }).argv;

  // If no command is specified, show top-level help string.
  if (!builtYargs._[0]) {
    yargs.showHelp();
  }
}
