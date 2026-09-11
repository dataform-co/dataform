import * as fs from "fs";
import parseDuration from "parse-duration";
import * as path from "path";
import yargs from "yargs";

import { CREDENTIALS_FILENAME } from "df/cli/api/commands/credentials";
import { actuallyResolve, assertPathExists } from "df/cli/util";
import { INamedOption } from "df/cli/yargswrapper";

export interface IProjectDirArgs {
  projectDir: string;
}

export const projectDirOption: INamedOption<yargs.PositionalOptions, IProjectDirArgs> = {
  name: "project-dir",
  option: {
    describe: "The Dataform project directory.",
    default: ".",
    coerce: actuallyResolve
  }
};

export const assertProjectDirExists = (argv: yargs.Arguments<IProjectDirArgs>) => {
  assertPathExists(argv.projectDir);
  const dataformJsonPath = path.resolve(argv.projectDir, "dataform.json");
  const workflowSettingsYamlPath = path.resolve(argv.projectDir, "workflow_settings.yaml");
  if (!fs.existsSync(dataformJsonPath) && !fs.existsSync(workflowSettingsYamlPath)) {
    throw new Error(
      `${argv.projectDir} does not appear to be a dataform directory (missing workflow_settings.yaml file).`
    );
  }
};

// Splits repeated and comma-separated values into a flat list, e.g.
// `--actions a,b --actions c` -> ["a", "b", "c"].
export const splitCommas = (raw: string[] | null) =>
  raw ? raw.map(value => value.split(",")).flat() : [];

export interface IActionsArgs {
  actions?: string[];
}

export const actionsOption: INamedOption<yargs.Options, IActionsArgs> = {
  name: "actions",
  option: {
    describe: "A list of action names or patterns to run. Can include '*' wildcards.",
    type: "array",
    coerce: splitCommas
  }
};

export interface ICredentialsArgs extends IProjectDirArgs {
  credentials: string;
}

export const credentialsOption: INamedOption<yargs.Options, ICredentialsArgs> = {
  name: "credentials",
  option: {
    describe: "The location of the credentials JSON file to use.",
    default: CREDENTIALS_FILENAME
  },
  check: (argv: yargs.Arguments<ICredentialsArgs>) =>
    actuallyResolve(argv.projectDir, argv.credentials)
};

export interface IJsonOutputArgs {
  json: boolean;
}

export const jsonOutputOption: INamedOption<yargs.Options, IJsonOutputArgs> = {
  name: "json",
  option: {
    describe: "Outputs a JSON representation of the compiled project or test results.",
    type: "boolean",
    default: false
  }
};

export const coerceTimeout = (rawTimeoutString: string | null) =>
  rawTimeoutString ? parseDuration(rawTimeoutString) : null;

export interface ITimeoutArgs {
  timeout: number | null;
}

export const timeoutOption: INamedOption<yargs.Options, ITimeoutArgs> = {
  name: "timeout",
  option: {
    describe: "Duration to allow project compilation to complete. Examples: '1s', '10m', etc.",
    type: "string",
    default: null,
    coerce: coerceTimeout
  }
};

// It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
export const requiresSelection = (
  name: string,
  actions: INamedOption<yargs.Options>,
  tags: INamedOption<yargs.Options>
): INamedOption<yargs.Options>["check"] => (argv: yargs.Arguments) => {
  if (argv[name] && !(argv[actions.name] || argv[tags.name])) {
    throw new Error(
      `The --${name} flag should only be supplied along with --${actions.name} or --${tags.name}.`
    );
  }
};

