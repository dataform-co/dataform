import * as chokidar from "chokidar";
import yargs from "yargs";

import { compile, prune } from "df/cli/api";
import {
  assertProjectDirExists,
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  projectDirOption,
  requiresSelection,
  splitCommas,
  timeoutOption
} from "df/cli/common_options";
import {
  compiledGraphOutputType,
  Logger,
  print,
  printCompiledGraph,
  printCompiledGraphErrors,
  printError
} from "df/cli/console";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { compiledGraphHasErrors } from "df/cli/util";
import { ICommand, INamedOption } from "df/cli/yargswrapper";

const RECOMPILE_DELAY = 500;

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
const outputActionsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-actions",
  option: {
    // No wildcard support: prune()'s matchPatterns() does exact matching on the
    // action name or its fully-qualified `database.schema.name`.
    describe: "A list of action names to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputTagsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-tags",
  option: {
    describe: "A list of tags to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputIncludeDepsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-deps",
  option: {
    describe: "If set, dependencies of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-deps", outputActionsOption, outputTagsOption)
};

const outputIncludeDependentsOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "output-include-dependents",
  option: {
    describe:
      "If set, dependents (downstream) of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-dependents", outputActionsOption, outputTagsOption)
};

const dotOutputOption: INamedOption<yargs.Options, ICompileArgs> = {
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

const quietCompileOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "quiet",
  option: {
    describe: "Less verbose compilation output. Example usage: 'dataform compile --quiet'",
    type: "boolean",
    default: false
  }
};

const watchOption: INamedOption<yargs.Options, ICompileArgs> = {
  name: "watch",
  option: {
    describe: "Whether to watch the changes in the project directory.",
    type: "boolean",
    default: false
  }
};

const verboseOption: INamedOption<yargs.Options, ICompileArgs> = {
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

export const compileCommand: ICommand<ICompileArgs> = {
  format: `compile [${projectDirOption.name}]`,
  description:
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
  positionalOptions: [projectDirOption],
  options: [
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
  ],
  check: [assertProjectDirExists],
  processFn: async argv => {
    const projectDir = argv.projectDir;
    const logger = new Logger(!argv.json);

    async function compileAndPrint() {
      let outputType = compiledGraphOutputType.Summary;
      if (argv.json) {
        outputType = compiledGraphOutputType.Json;
      } else if (argv.dot) {
        outputType = compiledGraphOutputType.Dot;
      }

      if (outputType === compiledGraphOutputType.Summary) {
        logger.log("Compiling...\n");
      }
      const compiledGraph = await compile({
        projectDir,
        projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
        timeoutMillis: argv.timeout || undefined,
        verbose: argv.verbose || false
      });

      // The whole project must compile (ref() resolution needs every action
      // registered), but the printed output can be filtered to the selected
      // action(s) -- mirroring how `run`/`build` prune the graph. We only prune
      // a clean graph; if compilation produced errors we print the full graph
      // plus the errors, keeping graph-level errors as-is.
      const hasSelector =
        (argv.outputActions?.length ?? 0) > 0 || (argv.outputTags?.length ?? 0) > 0;
      const outputGraph =
        hasSelector && !compiledGraphHasErrors(compiledGraph)
          ? prune(compiledGraph, {
              actions: argv.outputActions,
              tags: argv.outputTags,
              includeDependencies: argv.outputIncludeDeps,
              includeDependents: argv.outputIncludeDependents
            })
          : compiledGraph;
      printCompiledGraph(outputGraph, outputType, argv.quiet);
      if (compiledGraphHasErrors(compiledGraph)) {
        print("");
        printCompiledGraphErrors(compiledGraph.graphErrors, argv.quiet);
        return true;
      }
      return false;
    }

    const graphHasErrors = await compileAndPrint();

    if (!argv.watch) {
      return graphHasErrors ? 1 : 0;
    }

    let watching = true;

    let timeoutID: NodeJS.Timer = null;
    let isCompiling = false;

    // Initialize watcher.
    const watcher = chokidar.watch(projectDir, {
      ignored: /node_modules/,
      persistent: true,
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: 1000,
        pollInterval: 200
      }
    });

    const printReady = () => {
      print("\nWatching for changes...\n");
    };
    // Add event listeners.
    watcher
      .on("ready", printReady)
      .on("error", error => {
        // This error is caught not if there is a compilation error, but
        // if the watcher fails; this indicates an failure on our side.
        printError(`Error: ${error}`);
        process.exit(1);
      })
      .on("all", () => {
        if (timeoutID || isCompiling) {
          // don't recompile many times if we changed a lot of files
          clearTimeout(timeoutID);
        }

        timeoutID = setTimeout(async () => {
          clearTimeout(timeoutID);

          if (!isCompiling) {
            isCompiling = true;
            await compileAndPrint();
            printReady();
            isCompiling = false;
          }
        }, RECOMPILE_DELAY);
      });
    process.on("SIGINT", async () => {
      await watcher.close();
      watching = false;
      process.exit(1);
    });
    while (watching) {
      await new Promise((resolve, reject) => setTimeout(() => resolve(), 100));
    }
  }
};
