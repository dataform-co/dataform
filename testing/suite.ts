import { Hook, hook, IRunContext, Runner, test, Test } from "@dataform/testing";

export interface ISuiteOptions {
  name: string;
  parallel?: boolean;
}

export interface ISuiteContext {
  suite: typeof suite;
  test: typeof test;
  beforeEach: typeof hook;
  afterEach: typeof hook;
  before: typeof hook;
  after: typeof hook;
}

export function suite(name: string | ISuiteOptions, fn: (ctx?: ISuiteContext) => void): void;
export function suite(
  name: string,
  options: Omit<ISuiteOptions, "name">,
  fn: (ctx?: ISuiteContext) => void
): void;
export function suite(
  nameOrOptions: ISuiteOptions | string,
  optionsOrFn: Omit<ISuiteOptions, "name"> | ((ctx?: ISuiteContext) => void),
  fn?: (ctx?: ISuiteContext) => void
) {
  const suite = Suite.create(nameOrOptions, optionsOrFn, fn);
  if (Suite.globalStack.length > 0) {
    Suite.globalStack.slice(-1)[0].addSuite(suite);
  } else {
    Runner.registerTopLevelSuite(suite);
  }
}

export class Suite {
  public static readonly globalStack: Suite[] = [];

  public static create(
    nameOrOptions: ISuiteOptions | string,
    optionsOrFn: Omit<ISuiteOptions, "name"> | ((ctx?: ISuiteContext) => void),
    fn?: (ctx?: ISuiteContext) => void
  ): Suite {
    let options: ISuiteOptions =
      typeof nameOrOptions === "string" ? { name: nameOrOptions } : { ...nameOrOptions };
    if (typeof optionsOrFn === "function") {
      fn = optionsOrFn;
    } else {
      options = { ...options, ...optionsOrFn };
    }
    return new Suite(options, fn);
  }

  private suites: Suite[] = [];
  private tests: Test[] = [];

  private setUps: Hook[] = [];
  private tearDowns: Hook[] = [];
  private beforeEaches: Hook[] = [];
  private afterEaches: Hook[] = [];

  private runStarted: boolean;

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
    const path = this.options.name === undefined ? ctx.path : [...ctx.path, this.options.name];
    const runTestOrSuite = async (testOrSuite: Suite | Test) => {
      const testCtx = {
        ...ctx,
        path: [...path, testOrSuite.options.name]
      };
      try {
        for (const beforeEach of this.beforeEaches) {
          await beforeEach.run(testCtx);
        }
        await testOrSuite.run({ ...ctx, path });
      } catch (e) {
        // If a before each fails, we should still run the after eaches.
      }
      try {
        for (const afterEach of this.afterEaches) {
          await afterEach.run(testCtx);
        }
      } catch (e) {
        // If an after each fails, carry on.
      }
    };

    try {
      for (const setUp of this.setUps) {
        await setUp.run({ ...ctx, path });
      }

      if (this.options.parallel) {
        await Promise.all(testsAndSuites.map(testOrSuite => runTestOrSuite(testOrSuite)));
      } else {
        for (const testOrSuite of testsAndSuites) {
          await runTestOrSuite(testOrSuite);
        }
      }
    } catch (e) {
      // If a set up fails, still run the tear downs.
    }
    try {
      for (const tearDown of this.tearDowns) {
        await tearDown.run({ ...ctx, path });
      }
    } catch (e) {
      // If an tear down fails, carry on.
    }
  }

  public addSuite(suite: Suite) {
    this.checkMutation();
    this.suites.push(suite);
  }

  public addTest(test: Test) {
    this.checkMutation();
    this.tests.push(test);
  }

  private addHook(hookList: Hook[], hook: Hook) {
    this.checkMutation();
    hookList.push(hook);
  }

  private checkMutation() {
    if (this.runStarted) {
      throw new Error("Cannot add to a suite that has already started running.");
    }
    if (Suite.globalStack.slice(-1)[0] !== this) {
      throw new Error(
        "Cannot add to a suite that is not currently in scope. Call ctx.suite, ctx.test, ctx.before/after instead."
      );
    }
  }

  private context(): ISuiteContext {
    // The any types here are required due to method overloads.
    return {
      suite: ((...args: [any, any, any]) => this.addSuite(Suite.create(...args))) as typeof suite,
      test: ((...args: [any, any, any]) => this.addTest(Test.create(...args))) as typeof test,
      beforeEach: ((...args: [any, any, any]) =>
        this.addHook(this.beforeEaches, Hook.create(...args))) as typeof hook,
      afterEach: ((...args: [any, any, any]) =>
        this.addHook(this.afterEaches, Hook.create(...args))) as typeof hook,
      before: ((...args: [any, any, any]) =>
        this.addHook(this.setUps, Hook.create(...args))) as typeof hook,
      after: ((...args: [any, any, any]) =>
        this.addHook(this.tearDowns, Hook.create(...args))) as typeof hook
    };
  }
}
