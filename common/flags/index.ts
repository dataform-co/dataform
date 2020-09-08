import Long from "long";

export class Flags {
  public static boolean(name: string, defaultValue: boolean = false) {
    return new BooleanFlag(Flags.parsedArgv, name, defaultValue);
  }

  public static string(name: string, defaultValue: string = "") {
    return new StringFlag(Flags.parsedArgv, name, defaultValue);
  }

  public static number(name: string, defaultValue: number = 0) {
    return new NumberFlag(Flags.parsedArgv, name, defaultValue);
  }

  public static long(name: string, defaultValue: Long = Long.ZERO) {
    return new LongFlag(Flags.parsedArgv, name, defaultValue);
  }

  public static stringSet(name: string, defaultValue: Set<string> = new Set()) {
    return Flags.set(name, (singleValue: string) => singleValue, defaultValue);
  }

  public static set<T>(
    name: string,
    parser: (singleValue: string) => T,
    defaultValue: Set<T> = new Set()
  ) {
    return new SetFlag(Flags.parsedArgv, name, defaultValue, parser);
  }

  public static getRawFlagValue(flagName: string) {
    return Flags.parsedArgv[flagName];
  }

  public static setFlagValueForTesting(flagName: string, value: string) {
    Flags.parsedArgv[flagName] = value;
  }

  private static readonly parsedArgv = (() => {
    const parsedArgv: { [flagName: string]: string } = {};

    const splitArgv = [];
    for (let arg of process.argv) {
      // TODO: This is a temporary hack to be backwards-compatible with yargs behaviour, where
      // to switch off a boolean flag, it requires a 'no-' prefix in front of the flag name.
      if (arg.startsWith("--no-")) {
        arg = `--${arg.slice(5)}=false`;
      }
      if (arg.startsWith("--") && arg.includes("=")) {
        splitArgv.push(arg.slice(0, arg.indexOf("=")));
        splitArgv.push(arg.slice(arg.indexOf("=") + 1));
      } else {
        splitArgv.push(arg);
      }
    }

    let currentFlagName = "";
    for (const splitArg of splitArgv) {
      if (splitArg.startsWith("--")) {
        currentFlagName = splitArg.slice(2);
        parsedArgv[currentFlagName] = "";
      } else if (currentFlagName) {
        parsedArgv[currentFlagName] = splitArg;
        currentFlagName = "";
      }
    }

    return parsedArgv;
  })();
}

export interface IFlag<T> {
  get(): T;
}

abstract class AbstractFlag<T> implements IFlag<T> {
  private parsed: { [value: string]: T } = {};

  constructor(
    private readonly parsedFlags: { [flagName: string]: string },
    private readonly name: string,
    private readonly defaultValue: T
  ) {}
  public get(): T {
    if (!this.flagInArgv()) {
      return this.defaultValue;
    }
    const stringValue = this.stringValue();
    if (!(stringValue in this.parsed)) {
      this.parsed[stringValue] = this.parse(stringValue);
    }
    return this.parsed[stringValue];
  }

  protected abstract parse(stringValue: string): T;

  protected invalidFlagValueError() {
    return new Error(`Invalid flag value: ${this.stringValue()} [${this.name}]`);
  }

  private flagInArgv() {
    return this.name in this.parsedFlags;
  }

  private stringValue() {
    if (!this.flagInArgv()) {
      throw new Error(`Flag was not set: ${this.name}`);
    }
    return this.parsedFlags[this.name];
  }
}

class BooleanFlag extends AbstractFlag<boolean> {
  protected parse(stringValue: string): boolean {
    if (stringValue === "" || stringValue === "true") {
      return true;
    }
    if (stringValue === "false") {
      return false;
    }
    throw this.invalidFlagValueError();
  }
}

class StringFlag extends AbstractFlag<string> {
  protected parse(stringValue: string): string {
    return stringValue;
  }
}

class NumberFlag extends AbstractFlag<number> {
  protected parse(stringValue: string): number {
    return parseFloat(stringValue);
  }
}

class LongFlag extends AbstractFlag<Long> {
  protected parse(stringValue: string): Long {
    return Long.fromString(stringValue);
  }
}

class SetFlag<T> extends AbstractFlag<Set<T>> {
  constructor(
    parsedFlags: { [flagName: string]: string },
    name: string,
    defaultValue: Set<T>,
    private readonly parser: (singleValue: string) => T
  ) {
    super(parsedFlags, name, defaultValue);
  }

  protected parse(stringValue: string): Set<T> {
    return new Set(
      stringValue
        .split(",")
        .filter(singleValue => singleValue.length > 0)
        .map(this.parser)
    );
  }
}
