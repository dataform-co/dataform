import * as chokidar from "chokidar";
import yargs from "yargs";

import { compile, prune } from "df/cli/api";
import {
  jsonOutputOption,
  projectDirMustExistOption,
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
import { ProjectConfigOptions } from "df/cli/project_config_options";
import { compiledGraphHasErrors } from "df/cli/util";
import { ICommand, INamedOption } from "df/cli/yargswrapper";

const RECOMPILE_DELAY = 500;

// `compile` reuses the same prune() filtering as run/build, but these flags only
// filter the *printed output* -- the whole project still compiles. The `output-`
// prefix makes that distinction explicit.
const outputActionsOption: INamedOption<yargs.Options> = {
  name: "output-actions",
  option: {
    // No wildcard support: prune()'s matchPatterns() does exact matching on the
    // action name or its fully-qualified `database.schema.name`.
    describe: "A list of action names to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputTagsOption: INamedOption<yargs.Options> = {
  name: "output-tags",
  option: {
    describe: "A list of tags to filter the compiled output to.",
    type: "array",
    coerce: splitCommas
  }
};

const outputIncludeDepsOption: INamedOption<yargs.Options> = {
  name: "output-include-deps",
  option: {
    describe: "If set, dependencies of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-deps", outputActionsOption, outputTagsOption)
};

const outputIncludeDependentsOption: INamedOption<yargs.Options> = {
  name: "output-include-dependents",
  option: {
    describe:
      "If set, dependents (downstream) of the selected actions are also included in the output.",
    type: "boolean"
  },
  check: requiresSelection("output-include-dependents", outputActionsOption, outputTagsOption)
};

const dotOutputOption: INamedOption<yargs.Options> = {
  name: "dot",
  option: {
    describe: "Outputs a dot representation of the compiled project.",
    type: "boolean",
    default: false
  },
  check: (argv: yargs.Arguments<any>) => {
    if (argv.json && argv.dot) {
      throw new Error("Arguments --json and --dot are mutually exclusive.");
    }
  }
};

const watchOptionName = "watch";
const verboseOptionName = "verbose";

const quietCompileOption: INamedOption<yargs.Options> = {
  name: "quiet",
  option: {
    describe: "Less verbose compilation output. Example usage: 'dataform compile --quiet'",
    type: "boolean",
    default: false
  }
};

export const compileCommand: ICommand = {
  format: `compile [${projectDirMustExistOption.name}]`,
  description:
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
  positionalOptions: [projectDirMustExistOption],
  options: [
    {
      name: watchOptionName,
      option: {
        describe: "Whether to watch the changes in the project directory.",
        type: "boolean",
        default: false
      }
    },
    jsonOutputOption,
    dotOutputOption,
    timeoutOption,
    quietCompileOption,
    outputActionsOption,
    outputTagsOption,
    outputIncludeDepsOption,
    outputIncludeDependentsOption,
    {
      name: verboseOptionName,
      option: {
        describe: "Enable verbose compilation output. Example usage: 'dataform compile --verbose'",
        type: "boolean",
        default: false
      },
      check: (argv: yargs.Arguments) => {
        if (argv.quiet && argv.verbose) {
          throw new Error("Arguments --verbose and --quiet are mutually exclusive.");
        }
      }
    },
    ...ProjectConfigOptions.allYargsOptions
  ],
  processFn: async argv => {
    const projectDir = argv[projectDirMustExistOption.name];
    const logger = new Logger(!argv[jsonOutputOption.name]);

    async function compileAndPrint() {
      let outputType = compiledGraphOutputType.Summary;
      if (argv[jsonOutputOption.name]) {
        outputType = compiledGraphOutputType.Json;
      } else if (argv[dotOutputOption.name]) {
        outputType = compiledGraphOutputType.Dot;
      }

      if (outputType === compiledGraphOutputType.Summary) {
        logger.log("Compiling...\n");
      }
      const compiledGraph = await compile({
        projectDir,
        projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
        timeoutMillis: argv[timeoutOption.name] || undefined,
        verbose: argv[verboseOptionName] || false
      });

      // The whole project must compile (ref() resolution needs every action
      // registered), but the printed output can be filtered to the selected
      // action(s) -- mirroring how `run`/`build` prune the graph. We only prune
      // a clean graph; if compilation produced errors we print the full graph
      // plus the errors, keeping graph-level errors as-is.
      const hasSelector =
        argv[outputActionsOption.name]?.length > 0 || argv[outputTagsOption.name]?.length > 0;
      const outputGraph =
        hasSelector && !compiledGraphHasErrors(compiledGraph)
          ? prune(compiledGraph, {
              actions: argv[outputActionsOption.name],
              tags: argv[outputTagsOption.name],
              includeDependencies: argv[outputIncludeDepsOption.name],
              includeDependents: argv[outputIncludeDependentsOption.name]
            })
          : compiledGraph;
      printCompiledGraph(outputGraph, outputType, argv[quietCompileOption.name]);
      if (compiledGraphHasErrors(compiledGraph)) {
        print("");
        printCompiledGraphErrors(compiledGraph.graphErrors, argv[quietCompileOption.name]);
        return true;
      }
      return false;
    }

    const graphHasErrors = await compileAndPrint();

    if (!argv[watchOptionName]) {
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
