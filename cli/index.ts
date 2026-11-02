import { CLI_COMMANDS, createHelpCommand } from "df/cli/commands";
import { printError } from "df/cli/console";
import { handleTerminationSignals } from "df/cli/signals";
import { createYargsCli } from "df/cli/yargswrapper";

process.on("unhandledRejection", async (reason: any) => {
  printError(`Unhandled promise rejection: ${reason?.stack || reason}`);
});

export function runCli() {
  handleTerminationSignals();

  createYargsCli({
    commands: [createHelpCommand(CLI_COMMANDS), ...CLI_COMMANDS],
  });
}
