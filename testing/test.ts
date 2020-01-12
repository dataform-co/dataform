import { IRunContext, IRunResult, Runner, Suite } from "df/testing";

interface ITestOptions {
  name: string;
  timeout?: number;
}

export function test(
  nameOrOptions: ITestOptions | string,
  optionsOrFn: Omit<ITestOptions, "name"> | (() => void),
  fn?: () => void
): void {
  Suite.globalStack.slice(-1)[0].addTest(Test.create(nameOrOptions, optionsOrFn, fn));
  Runner.queueRunAndExit();
}

export class Test {
  public static readonly DEFAULT_TIMEOUT = 30000;

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

  constructor(public readonly options: ITestOptions, private readonly fn: () => any) {}

  public async run(ctx: IRunContext) {
    let timer: NodeJS.Timer;
    const timeout = this.options.timeout || Test.DEFAULT_TIMEOUT;
    const result: Partial<IRunResult> = {
      path: [...ctx.path, this.options.name]
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

    ctx.results.push(result as IRunResult);
  }
}
