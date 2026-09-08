import { ICommand } from "df/cli/yargswrapper";

// This dummy command is a hack with the only goal of displaying "help" as a command in the CLI
// and we need it because of the limitations of yargs considering "help" as an option and not as a command.
export const helpCommand: ICommand = {
  format: "help [command]",
  description: "Show help. If [command] is specified, the help is for the given command.",
  positionalOptions: [],
  options: [],
  processFn: async () => {
    return 0;
  }
};
