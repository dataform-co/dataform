import * as fs from "fs";
import parseDuration from "parse-duration";
import * as path from "path";
import yargs from "yargs";

import { CREDENTIALS_FILENAME } from "df/cli/api/commands/credentials";
import { DEFAULT_COMPILATION_TIMEOUT_MILLIS } from "df/cli/api/utils/constants";
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
    coerce: actuallyResolve,
  },
};

export const assertProjectDirExists = (argv: yargs.Arguments<IProjectDirArgs>) => {
  assertPathExists(argv.projectDir);
  const dataformJsonPath = path.resolve(argv.projectDir, "dataform.json");
  const workflowSettingsYamlPath = path.resolve(argv.projectDir, "workflow_settings.yaml");
  if (!fs.existsSync(dataformJsonPath) && !fs.existsSync(workflowSettingsYamlPath)) {
    throw new Error(
      `${argv.projectDir} does not appear to be a dataform directory (missing workflow_settings.yaml file).`,
    );
  }
};

// Splits repeated and comma-separated values into a flat list, e.g.
// `--actions a,b --actions c` -> ["a", "b", "c"].
export const splitCommas = (raw: string[] | null) =>
  raw ? raw.map((value) => value.split(",")).flat() : [];

export interface IActionsArgs {
  actions?: string[];
}

export const actionsOption: INamedOption<yargs.Options, IActionsArgs> = {
  name: "actions",
  option: {
    // No wildcard support: prune()'s matchPatterns() does exact matching on the action name or
    // its fully-qualified `database.schema.name` (or 4-part `database.catalog.namespace.name`).
    describe:
      "Actions to run (comma-separated or repeatable). Each entry is an action name or a " +
      "fully-qualified database.schema.name (or 4-part database.catalog.namespace.name) target.",
    type: "array",
    coerce: splitCommas,
  },
};

export interface ICredentialsArgs extends IProjectDirArgs {
  credentials: string;
}

export const credentialsOption: INamedOption<yargs.Options, ICredentialsArgs> = {
  name: "credentials",
  option: {
    describe: "Path to the credentials JSON file to use.",
    type: "string",
    default: CREDENTIALS_FILENAME,
  },
  check: (argv: yargs.Arguments<ICredentialsArgs>) =>
    actuallyResolve(argv.projectDir, argv.credentials),
};

export interface IJsonOutputArgs {
  json: boolean;
}

export const jsonOutputOption: INamedOption<yargs.Options, IJsonOutputArgs> = {
  name: "json",
  option: {
    describe: "Outputs the result as JSON instead of human-readable text.",
    type: "boolean",
    default: false,
  },
};

export const coerceTimeout = (rawTimeoutString: string | null) =>
  rawTimeoutString ? parseDuration(rawTimeoutString) : null;

export interface ITimeoutArgs {
  timeout: number | null;
}

export const timeoutOption: INamedOption<yargs.Options, ITimeoutArgs> = {
  name: "timeout",
  option: {
    describe: "Duration to allow project compilation to complete, e.g. '1s', '10m'.",
    type: "string",
    default: null,
    coerce: coerceTimeout,
  },
  docs: { default: `\`'${DEFAULT_COMPILATION_TIMEOUT_MILLIS / 1000}s'\`` },
};

// It would be nice to use yargs' "implies" to implement this, but it doesn't work for some reason.
export const requiresSelection =
  (name: string, actions: { name: string }, tags: { name: string }) => (argv: yargs.Arguments) => {
    if (argv[name] && !(argv[actions.name] || argv[tags.name])) {
      throw new Error(
        `The --${name} flag should only be supplied along with --${actions.name} or --${tags.name}.`,
      );
    }
  };
