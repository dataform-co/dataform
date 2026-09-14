import yargs from "yargs";

import { printError } from "df/cli/console";

export interface ICli {
  commands: ICommandBase[];
}

export interface ICommandBase {
  format: string;
  description: string;
  positionalOptions: Array<INamedOption<yargs.PositionalOptions, any>>;
  options: Array<INamedOption<yargs.Options, any>>;
  check?: Array<(argv: yargs.Arguments<any>) => void>;
  processFn: (argv: yargs.Arguments<any>) => Promise<number>;
}

export interface ICommand<TArgs> extends ICommandBase {
  positionalOptions: Array<INamedOption<yargs.PositionalOptions, TArgs>>;
  options: Array<INamedOption<yargs.Options, TArgs>>;
  check?: Array<(argv: yargs.Arguments<TArgs>) => void>;
  processFn: (argv: yargs.Arguments<TArgs>) => Promise<number>;
}

export interface INamedOption<TOption, TArgs> {
  name: string;
  option: TOption;
  check?: (args: yargs.Arguments<TArgs>) => void;
}

export function createYargsCli(cli: ICli) {
  const yargsInstance = setupYargs(cli.commands, process.argv.slice(2))
    .strict()
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
    });

  const parsed = yargsInstance.argv;
  if (!parsed._[0]) {
    yargs.showHelp();
  }
  return yargsInstance;
}

export function setupYargs(commands: ICommandBase[], args: string[]) {
  let yargsChain = yargs(args).scriptName("dataform").wrap(null);
  for (const command of commands) {
    yargsChain = yargsChain.command(
      command.format,
      command.description,
      (yargsChainer: yargs.Argv) => buildCommand(yargsChainer, command),
      async (argv: any) => {
        const exitCode = await command.processFn(argv);
        process.exit(exitCode);
      }
    );
  }
  return yargsChain;
}

function buildCommand(yargsChain: yargs.Argv, command: ICommandBase) {
  const checks: Array<(args: yargs.Arguments<any>) => void> = [];

  if (command.check) {
    checks.push(...command.check);
  }

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
