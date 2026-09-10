import {
  compileCommand,
  createHelpCommand,
  formatCommand,
  initCommand,
  initCredsCommand,
  installCommand,
  runCommand,
  testCommand
} from "df/cli/commands";
import { printError } from "df/cli/console";
import { createYargsCli, ICommand } from "df/cli/yargswrapper";

process.on("unhandledRejection", async (reason: any) => {
  printError(`Unhandled promise rejection: ${reason?.stack || reason}`);
});

// TODO: Since yargs launched an actually well typed API in version 12, let's use it as this file is currently not type checked.
export function runCli() {
  const commands: ICommand[] = [
    initCommand,
    installCommand,
    initCredsCommand,
    compileCommand,
    testCommand,
    runCommand,
    formatCommand
  ];

  createYargsCli({
    commands: [createHelpCommand(commands), ...commands]
  });
}
