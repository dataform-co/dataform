import yargs from "yargs";

import { maybeConfigureAnalytics, trackCommand } from "df/cli/analytics";

export interface ICli {
  commands: ICommand[];
}

export interface ICommand {
  format: string;
  description: string;
  positionalOptions: Array<INamedOption<yargs.PositionalOptions>>;
  options: Array<INamedOption<yargs.Options>>;
  processFn: (argv: { [argumentName: string]: any }) => Promise<number>;
}

export interface INamedOption<T> {
  name: string;
  option: T;
  check?: (args: yargs.Arguments) => void;
}

export function createYargsCli(cli: ICli) {
  let yargsChain = yargs(fixArgvForHelp());
  for (const command of cli.commands) {
    yargsChain = yargsChain.command(
      command.format,
      command.description,
      (yargsChainer: yargs.Argv) => createOptionsChain(yargsChainer, command),
      async (argv: { [argumentName: string]: any }) => {
        await maybeConfigureAnalytics();
        const analyticsTrack = trackCommand(command.format.split(" ")[0]);
        const exitCode = await command.processFn(argv);
        let timer: NodeJS.Timer;
        // Analytics tracking can take a while, so wait up to 2 seconds for them to finish.
        await Promise.race([
          analyticsTrack,
          new Promise(resolve => (timer = setTimeout(resolve, 2000)))
        ]);
        clearTimeout(timer);
        process.exit(exitCode);
      }
    );
  }
  return yargsChain;
}

function createOptionsChain(yargsChain: yargs.Argv, command: ICommand) {
  const checks: Array<(args: yargs.Arguments) => void> = [];

  for (const positionalOption of command.positionalOptions) {
    yargsChain = yargsChain.positional(positionalOption.name, positionalOption.option);
    if (positionalOption.check) {
      checks.push(positionalOption.check);
    }
  }
  for (const option of command.options) {
    yargsChain = yargsChain.option(option.name, option.option);
    if (option.check) {
      checks.push(option.check);
    }
  }
  yargsChain = yargsChain.check(argv => {
    checks.forEach(check => check(argv));
    return true;
  });
  return yargsChain;
}

function fixArgvForHelp() {
  // Obviously this is a massive hack.
  // The outcome of this is that the following commands are interchangeable:
  // $ dataform help run
  // $ dataform --help run
  // The problem is that yargs.help() only allows us to specify an alias for the "--help" built-in option (by default that alias is "help").
  // But because "--help" is only an option, not a command, it appears to be impossible (?) to configure yargs to respond to "help" correctly
  // (or at least, to correctly print help strings for commands; it happily prints a top-level help string).
  const argvCopy = process.argv.slice(2);
  if (argvCopy[0] === "help") {
    argvCopy[0] = "--help";
  }
  return argvCopy;
}
