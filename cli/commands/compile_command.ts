import * as chokidar from "chokidar";

import { compile, prune } from "df/cli/api";
import { compileOptions, ICompileArgs } from "df/cli/commands/compile_options";
import { assertProjectDirExists, projectDirOption } from "df/cli/common_options";
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
import { ICommand } from "df/cli/yargswrapper";

const RECOMPILE_DELAY = 500;

export const compileCommand: ICommand<ICompileArgs> = {
  format: `compile [${projectDirOption.name}]`,
  description:
    "Compile the dataform project. Produces JSON output describing the non-executable graph.",
  positionalOptions: [projectDirOption],
  options: compileOptions,
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
    return 0;
  }
};
