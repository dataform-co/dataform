import yargs from "yargs";

import { ICommand, setupYargs } from "df/cli/yargswrapper";

export function createHelpCommand(commands: ICommand[]): ICommand {
  const helpCmd: ICommand = {
    format: "help [command]",
    description: "Show help. If [command] is specified, the help is for the given command.",
    positionalOptions: [],
    options: [],
    processFn: async argv => {
      if (argv.command) {
        setupYargs([helpCmd, ...commands], [argv.command, "--help"]).parse();
      } else {
        yargs.showHelp();
      }
      return 0;
    }
  };
  return helpCmd;
}

