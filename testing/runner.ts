import chalk from "chalk";
import { Suite } from "df/testing";
import { promisify } from "util";

export interface IRunResult {
  path: string[];
  err: any;
  outcome: "passed" | "timeout" | "failed";
}

export interface IRunContext {
  path: string[];
  results: IRunResult[];
}

export class Runner {
  public static setNoExit(noExit: boolean) {
    Runner.noExit = noExit;
  }
  public static queueRunAndExit() {
    if (!Runner.resultPromise) {
      Runner.resultPromise = Runner.run();
    }
  }

  // tslint:disable: no-console
  public static async run() {
    // We tell the runner to start running at the end of current block of
    // synchronously executed code. This will typically be after all the
    // suite definitions are evaluated.
    await promisify(process.nextTick)();
    const ctx: IRunContext = {
      path: [],
      results: []
    };

    await Suite.globalStack[0].run(ctx);

    if (ctx.results.length === 0) {
      ctx.results.push({
        path: [],
        err: new Error("No tests found in top level test suite."),
        outcome: "failed"
      });
    }
    const indent = (value: string, levels = 4) =>
      value
        .split("\n")
        .map(line => `${" ".repeat(4).join("")}${line}`)
        .join("\n");

    for (const result of ctx.results) {
      const outcomeString = (result.outcome || "unknown").toUpperCase();
      const pathString = result.path.join(" > ");

      const colorFn =
        result.outcome === "failed" || result.outcome === "timeout"
          ? chalk.red
          : result.outcome === "passed"
          ? chalk.green
          : chalk.yellow;
      if (pathString.length + outcomeString.length + 1 <= 80) {
        console.info(
          `${pathString}${new Array(80 - pathString.length - outcomeString.length - 1)
            .fill(" ")
            .join("")}${colorFn(outcomeString)}`
        );
      } else {
        console.info(pathString);
        console.info(
          `${new Array(80 - outcomeString.length - 1).fill(" ").join("")}${colorFn(outcomeString)}`
        );
      }

      if (result.err) {
        const errString = result.err.stack
          ? result.err.stack && indent(result.err.stack as string)
          : `    ${JSON.stringify(result.err, null, 4)}`;

        console.error(`\n${errString}\n`);
        if (result.err.showDiff) {
          console.error(`    Expected:\n`);
          console.error(indent(JSON.stringify(result.err.expected, null, 4), 8));
          console.error(`\n    Actual:\n`);
          console.error(indent(JSON.stringify(result.err.actual, null, 4), 8));
          console.error("\n");
        }
      }
    }

    const hasErrors = ctx.results.some(result => result.outcome !== "passed");

    if (hasErrors) {
      console.log(`\nTests failed.`);
    } else {
      console.log(`\nTests passed.`);
    }

    process.exitCode = hasErrors ? 1 : 0;

    if (!Runner.noExit) {
      process.exit();
    }

    return ctx.results;
  }

  public static async result() {
    return await Runner.resultPromise;
  }
  private static noExit = false;

  private static resultPromise: Promise<IRunResult[]>;
}
