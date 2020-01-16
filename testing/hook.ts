import { IRunContext, IRunResult } from "df/testing";

export type IHookFunction = () => any;

export interface IHookOptions {
  name: string;
  timeout?: number;
}

export type IHookHandler = (
  nameOrOptions: IHookOptions | string,
  optionsOrFn: Omit<IHookOptions, "name"> | IHookFunction,
  fn?: IHookFunction
) => void;

export class Hook {
  public static readonly DEFAULT_TIMEOUT_MILLIS = 30000;

  public static create(
    nameOrOptions: IHookOptions | string,
    optionsOrFn: Omit<IHookOptions, "name"> | IHookFunction,
    fn?: IHookFunction
  ) {
    let options: IHookOptions = typeof nameOrOptions === "string" ? { name: nameOrOptions } : nameOrOptions;
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
    return new Hook(options, fn);
  }

  constructor(public readonly options: IHookOptions, private readonly fn: IHookFunction) {}

  public async run(ctx: IRunContext) {
    let timer: NodeJS.Timer;
    const timeout = this.options.timeout || Hook.DEFAULT_TIMEOUT;
    const result: Partial<IRunResult> = {
      path: [...ctx.path, `${this.options.name} (hook)`]
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
      ctx.results.push(result as IRunResult);
      // If hooks fail, we throw anyway.
      throw e;
    }

    clearTimeout(timer);
  }
}
