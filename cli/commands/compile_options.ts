import yargs from "yargs";

import {
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  requiresSelection,
  splitCommas,
  timeoutOption,
} from "df/cli/common_options";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { INamedOption } from "df/cli/yargswrapper";

export interface ICompileArgs
  extends IProjectDirArgs, IProjectConfigArgs, IJsonOutputArgs, ITimeoutArgs {
  watch: boolean;
  dot: boolean;
  quiet: boolean;
  outputActions?: string[];
  outputTags?: string[];
  outputIncludeDeps?: boolean;
  outputIncludeDependents?: boolean;
  verbose: boolean;
}

// `compile` reuses the same prune() filtering as run/build, but these flags only
// filter the *printed output* -- the whole project still compiles. The `output-`
// prefix makes that distinction explicit.
export const outputActionsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-actions",
  option: {
    // No wildcard support: prune()'s matchPatterns() does exact matching on the
    // action name or its fully-qualified `database.schema.name` (or 4-part
    // `database.catalog.namespace.name`).
    describe:
      "Filters the printed output to these actions (comma-separated or repeatable). Each entry " +
      "is an action name or a fully-qualified database.schema.name (or 4-part " +
      "database.catalog.namespace.name) target. The whole project is still compiled.",
    type: "array",
    coerce: splitCommas,
  },
};

export const outputTagsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-tags",
  option: {
    describe:
      "Filters the printed output to actions with any of these tags (comma-separated or " +
      "repeatable).",
    type: "array",
    coerce: splitCommas,
  },
};

export const outputIncludeDepsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-deps",
  option: {
    describe:
      "Also includes upstream dependencies of the selected actions in the output. Requires " +
      "--output-actions or --output-tags.",
    type: "boolean",
  },
  check: requiresSelection("output-include-deps", outputActionsOption, outputTagsOption),
};

export const outputIncludeDependentsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-dependents",
  option: {
    describe:
      "Also includes downstream dependents of the selected actions in the output. Requires " +
      "--output-actions or --output-tags.",
    type: "boolean",
  },
  check: requiresSelection("output-include-dependents", outputActionsOption, outputTagsOption),
};

export const dotOutputOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "dot",
  option: {
    describe:
      "Outputs a Graphviz DOT representation of the compiled project. Mutually exclusive with " +
      "--json.",
    type: "boolean",
    default: false,
  },
  check: (argv: yargs.Arguments<ICompileArgs>) => {
    if (argv.json && argv.dot) {
      throw new Error("Arguments --json and --dot are mutually exclusive.");
    }
  },
};

export const quietCompileOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "quiet",
  option: {
    describe: "Less verbose compilation output. Mutually exclusive with --verbose.",
    type: "boolean",
    default: false,
  },
};

export const watchOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "watch",
  option: {
    describe: "Watches the project directory and recompiles on changes.",
    type: "boolean",
    default: false,
  },
};

export const verboseOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "verbose",
  option: {
    describe: "Enables verbose compilation output. Mutually exclusive with --quiet.",
    type: "boolean",
    default: false,
  },
  check: (argv: yargs.Arguments<ICompileArgs>) => {
    if (argv.quiet && argv.verbose) {
      throw new Error("Arguments --verbose and --quiet are mutually exclusive.");
    }
  },
};

export const compileOptions: Array<INamedOption<yargs.Options, ICompileArgs>> = [
  jsonOutputOption,
  dotOutputOption,
  verboseOption,
  quietCompileOption,
  watchOption,
  timeoutOption,
  outputActionsOption,
  outputTagsOption,
  outputIncludeDepsOption,
  outputIncludeDependentsOption,
  ...ProjectConfigOptions.allYargsOptions,
];
