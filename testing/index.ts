import { default as chalk } from "chalk";
import { promisify } from "util";

export type IHookFunction = () => any;

export interface IHook {
  name: string;
  fn: IHookFunction;
}

export type IHookHandler = (nameOrFn: string | IHookFunction, fn?: IHookFunction) => any;

export interface IRunResult {
  path: string[];
  err: any;
  outcome: "passed" | "timeout" | "failed";
  duration?: number;
}

interface IRunContext {
  path: string[];
  results: IRunResult[];
}

export interface ISuiteOptions {
  name: string;
  parallel?: boolean;
}

export interface ISuiteContext {
  suite: typeof suite;
  test: typeof test;
  beforeEach: IHookHandler;
  afterEach: IHookHandler;
  setUp: IHookHandler;
  tearDown: IHookHandler;
}

class Suite {
  public static readonly globalStack: Suite[] = [new Suite({ name: "" }, () => null)];

  public static create(
    nameOrOptions: ISuiteOptions | string,
    optionsOrFn: Omit<ISuiteOptions, "name"> | ((ctx?: ISuiteContext) => void),
    fn?: (ctx?: ISuiteContext) => void
  ) {
    let options: ISuiteOptions = { name: null };
    if (typeof nameOrOptions === "string") {
      options.name = nameOrOptions;
    } else {
      options = { ...nameOrOptions };
    }
    if (typeof optionsOrFn === "function") {
      fn = optionsOrFn;
    } else {
      options = { ...options, ...optionsOrFn };
    }
    return new Suite(options, fn);
  }

  private suites: Suite[] = [];
  private tests: Test[] = [];

  private setUps: IHook[] = [];
  private tearDowns: IHook[] = [];
  private beforeEaches: IHook[] = [];
  private afterEaches: IHook[] = [];
  constructor(public readonly options: ISuiteOptions, fn: (ctx?: ISuiteContext) => void) {
    if (Suite.globalStack) {
      Suite.globalStack.push(this);
    }
    fn(this.context());
    if (Suite.globalStack) {
      Suite.globalStack.pop();
    }
  }

  public async run(ctx: IRunContext) {
    const testsAndSuites = [...this.suites, ...this.tests];
    const path = [...ctx.path, this.options.name];
    const runTestOrSuite = async (testOrSuite: Suite | Test) => {
      for (const beforeEach of this.beforeEaches) {
        await beforeEach.fn();
      }
      await testOrSuite.run({ ...ctx, path });
      for (const afterEach of this.afterEaches) {
        await afterEach.fn();
      }
    };

    try {
      for (const setUp of this.setUps) {
        await setUp.fn();
      }

      if (this.options.parallel) {
        await Promise.all(testsAndSuites.map(testOrSuite => runTestOrSuite(testOrSuite)));
      } else {
        for (const testOrSuite of testsAndSuites) {
          await runTestOrSuite(testOrSuite);
        }
      }
      for (const tearDown of this.tearDowns) {
        await tearDown.fn();
      }
    } catch (e) {
      ctx.results.push({
        path,
        outcome: "failed",
        err: e
      });
    }
  }

  public addSuite(suite: Suite) {
    this.suites.push(suite);
  }

  public addTest(test: Test) {
    this.tests.push(test);
  }

  private context(): ISuiteContext {
    const getHook: (nameOrFn: string | IHookFunction, fn?: IHookFunction) => IHook = (
      nameOrFn,
      fn
    ) => {
      return {
        name: typeof nameOrFn === "string" ? nameOrFn : "unknown",
        fn: typeof nameOrFn === "function" ? nameOrFn : fn
      };
    };
    return {
      suite: (a, b, c) => this.addSuite(Suite.create(a, b, c)),
      test: (a, b, c) => this.addTest(Test.create(a, b, c)),
      beforeEach: (a, b) => this.beforeEaches.push(getHook(a, b)),
      afterEach: (a, b) => this.afterEaches.push(getHook(a, b)),
      setUp: (a, b) => this.setUps.push(getHook(a, b)),
      tearDown: (a, b) => this.tearDowns.push(getHook(a, b))
    };
  }
}

export function test(
  nameOrOptions: ITestOptions | string,
  optionsOrFn: Omit<ITestOptions, "name"> | (() => void),
  fn?: () => void
) {
  Suite.globalStack.slice(-1)[0].addTest(Test.create(nameOrOptions, optionsOrFn, fn));
  Runner.queueRunAndExit();
}

export function suite(
  nameOrOptions: ISuiteOptions | string,
  optionsOrFn: Omit<ISuiteOptions, "name"> | ((ctx?: ISuiteContext) => void),
  fn?: (ctx?: ISuiteContext) => void
) {
  Suite.globalStack.slice(-1)[0].addSuite(Suite.create(nameOrOptions, optionsOrFn, fn));
  Runner.queueRunAndExit();
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
    for (const result of ctx.results) {
      // tslint:disable-next-line: no-console

      const outcomeString = (result.outcome || "unknown").toUpperCase();
      const pathString = result.path.join(" > ");

      const colorFn =
        result.outcome === "failed" || result.outcome === "timeout"
          ? chalk.red
          : result.outcome === "passed"
          ? chalk.green
          : chalk.yellow;
      console.info(
        `${pathString}${new Array(Math.max(80 - pathString.length - outcomeString.length - 1, 1))
          .fill(" ")
          .join("")}${colorFn(outcomeString)}`
      );
      if (result.err) {
        const errString = result.err.stack
          ? (
              result.err.stack &&
              (result.err.stack as string).split("\n").map(line => `    ${line}`)
            ).join("\n")
          : `    ${JSON.stringify(result.err, null, 4)}`;

        // tslint:disable-next-line: no-console
        console.error(`\n${errString}\n\n`);
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

interface ITestOptions {
  name: string;
  timeout?: number;
}

export class Test {
  public static readonly DEFAULT_TIMEOUT = 10000;

  public static create(
    nameOrOptions: ITestOptions | string,
    optionsOrFn: Omit<ITestOptions, "name"> | (() => void),
    fn?: () => void
  ) {
    let options: ITestOptions = { name: null };
    if (typeof nameOrOptions === "string") {
      options.name = nameOrOptions;
    } else {
      options = { ...nameOrOptions };
    }
    if (typeof optionsOrFn === "function") {
      fn = optionsOrFn;
    } else {
      options = { ...options, ...optionsOrFn };
    }
    return new Test(options, fn);
  }

  constructor(private readonly options: ITestOptions, private readonly fn: () => any) {}

  public async run(ctx: IRunContext) {
    let timer: NodeJS.Timer;
    const startMillis = Date.now();
    const timeout = this.options.timeout || Test.DEFAULT_TIMEOUT;
    const result: Partial<IRunResult> = {
      path: [...ctx.path, this.options.name].filter(v => !!v)
    };
    try {
      await Promise.race([
        this.fn(),
        new Promise((_, reject) => {
          timer = setTimeout(() => {
            result.outcome = "timeout";
            reject(new Error(`Timed out (${timeout}ms).`));
          }, timeout);
        })
      ]);
      result.outcome = "passed";
    } catch (e) {
      if (result.outcome !== "timeout") {
        result.outcome = "failed";
      }
      result.err = e;
    }

    clearTimeout(timer);

    result.duration = Date.now() - startMillis;
    ctx.results.push(result as IRunResult);
  }
}
