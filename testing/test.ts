import { IRunContext, IRunResult, Runner, Suite } from "@dataform/testing";

interface ITestOptions {
  name: string;
  timeout?: number;
  retries?: number;
}

export function test(name: string | ITestOptions, fn: (ctx?: ITestOptions) => void): void;
export function test(
  name: string,
  options: Omit<ITestOptions, "name">,
  fn: (ctx?: ITestOptions) => void
): void;
export function test(
  nameOrOptions: ITestOptions | string,
  optionsOrFn: Omit<ITestOptions, "name"> | (() => void),
  fn?: () => void
): void {
  const test = Test.create(nameOrOptions, optionsOrFn, fn);
  if (Suite.globalStack.length > 0) {
    Suite.globalStack.slice(-1)[0].addTest(test);
  } else {
    throw new Error("Cannot create a top level test, must be created in a suite.");
  }
}

export class Test {
  public static readonly DEFAULT_TIMEOUT_MILLIS = 30000;

  public static create(
    nameOrOptions: ITestOptions | string,
    optionsOrFn: Omit<ITestOptions, "name"> | (() => void),
    fn?: () => void
  ) {
    let options: ITestOptions =
      typeof nameOrOptions === "string" ? { name: nameOrOptions } : { ...nameOrOptions };
    if (typeof optionsOrFn === "function") {
      fn = optionsOrFn;
    } else {
      options = { ...options, ...optionsOrFn };
    }
    return new Test(options, fn);
  }

  constructor(public readonly options: ITestOptions, private readonly fn: () => any) {}

  public async run(ctx: IRunContext) {
    let lastResult: IRunResult;
    const retries = this.options.retries || 0;
    for (let i = 0; i <= retries; i++) {
      let timer: NodeJS.Timer;
      const timeout = this.options.timeout || Test.DEFAULT_TIMEOUT_MILLIS;
      const result: IRunResult = {
        path: [...ctx.path, this.options.name],
        outcome: "failed"
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
        result.err = e;
      } finally {
        clearTimeout(timer);
      }

      lastResult = result;
      if (result.outcome === "passed") {
        break;
      }
    }

    ctx.results.push(lastResult);
  }
}
