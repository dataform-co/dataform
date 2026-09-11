import yargs from "yargs";

import {
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  requiresSelection,
  splitCommas,
  timeoutOption
} from "df/cli/common_options";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { INamedOption } from "df/cli/yargswrapper";

export interface ICompileArgs
  extends IProjectDirArgs,
    IProjectConfigArgs,
    IJsonOutputArgs,
    ITimeoutArgs {
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
    // action name or its fully-qualified `database.schema.name`.
    describe: "A list of action names to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

export const outputTagsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-tags",
  option: {
    describe: "A list of tags to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

export const outputIncludeDepsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-deps",
  option: {
    describe: "If set, dependencies of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-deps", outputActionsOption, outputTagsOption)
};

export const outputIncludeDependentsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-dependents",
  option: {
    describe:
      "If set, dependents (downstream) of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-dependents", outputActionsOption, outputTagsOption)
};

export const dotOutputOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "dot",
  option: {
    describe: "Outputs a dot representation of the compiled project.",
    type: "boolean",
    default: false
  },
  check: (argv: yargs.Arguments<ICompileArgs>) => {
    if (argv.json && argv.dot) {
      throw new Error("Arguments --json and --dot are mutually exclusive.");
    }
  }
};

export const quietCompileOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "quiet",
  option: {
    describe: "Less verbose compilation output. Example usage: 'dataform compile --quiet'",
    type: "boolean",
    default: false
  }
};

export const watchOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "watch",
  option: {
    describe: "Whether to watch the changes in the project directory.",
    type: "boolean",
    default: false
  }
};

export const verboseOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "verbose",
  option: {
    describe: "Enable verbose compilation output. Example usage: 'dataform compile --verbose'",
    type: "boolean",
    default: false
  },
  check: (argv: yargs.Arguments<ICompileArgs>) => {
    if (argv.quiet && argv.verbose) {
      throw new Error("Arguments --verbose and --quiet are mutually exclusive.");
    }
  }
};

export const compileOptions: Array<INamedOption<yargs.Options, ICompileArgs>> = [
  watchOption,
  verboseOption,
  jsonOutputOption,
  dotOutputOption,
  timeoutOption,
  quietCompileOption,
  outputActionsOption,
  outputTagsOption,
  outputIncludeDepsOption,
  outputIncludeDependentsOption,
  ...ProjectConfigOptions.allYargsOptions
];
