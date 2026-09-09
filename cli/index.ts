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
