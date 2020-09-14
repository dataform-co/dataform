import Long from "long";

export class Flags {
  public static boolean(name: string, defaultValue: boolean = false) {
    return new SingleValueFlag(name, defaultValue, (stringValue: string) => {
      if (stringValue === "" || stringValue === "true") {
        return true;
      }
      if (stringValue === "false") {
        return false;
      }
      throw Flags.invalidFlagValueError(name);
    });
  }

  public static string(name: string, defaultValue: string = "") {
    return new SingleValueFlag(name, defaultValue, (stringValue: string) => stringValue);
  }

  public static number(name: string, defaultValue: number = 0) {
    return new SingleValueFlag(name, defaultValue, (stringValue: string) =>
      parseFloat(stringValue)
    );
  }

  public static long(name: string, defaultValue: Long = Long.ZERO) {
    return new SingleValueFlag(name, defaultValue, Long.fromString);
  }

  public static stringSet(name: string, defaultValue: Set<string> = new Set()) {
    return new SetFlag(name, defaultValue, (singleValue: string) => singleValue);
  }

  public static getRawFlagValue(flagName: string) {
    return Flags.parsedArgv[flagName];
  }

  public static setRawFlagValueForTesting(flagName: string, value: string) {
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

    let flagsStarted = false;
    let currentFlagName = "";
    for (const splitArg of splitArgv) {
      if (splitArg.startsWith("--")) {
        flagsStarted = true;
        currentFlagName = splitArg.slice(2);
        parsedArgv[currentFlagName] = "";
      } else if (currentFlagName) {
        parsedArgv[currentFlagName] = splitArg;
        currentFlagName = "";
      } else if (flagsStarted) {
        throw new Error(`Arg neither flag name nor flag value: ${splitArg}`);
      }
    }

    return parsedArgv;
  })();

  private static invalidFlagValueError(flagName: string) {
    return new Error(`Invalid flag value: ${Flags.getRawFlagValue(flagName)} [${flagName}]`);
  }
}

export interface IFlag<T> {
  get(): T;
}

abstract class AbstractFlag<T> implements IFlag<T> {
  private parsed: { [value: string]: T } = {};

  constructor(private readonly name: string, private readonly defaultValue: T) {}
  public get(): T {
    if (Flags.getRawFlagValue(this.name) === undefined) {
      return this.defaultValue;
    }
    const stringValue = Flags.getRawFlagValue(this.name);
    if (!(stringValue in this.parsed)) {
      this.parsed[stringValue] = this.parse(stringValue);
    }
    return this.parsed[stringValue];
  }

  protected abstract parse(stringValue: string): T;
}

class SingleValueFlag<T> extends AbstractFlag<T> {
  constructor(name: string, defaultValue: T, private readonly parser: (singleValue: string) => T) {
    super(name, defaultValue);
  }

  protected parse(stringValue: string): T {
    return this.parser(stringValue);
  }
}

class SetFlag<T> extends AbstractFlag<Set<T>> {
  constructor(
    name: string,
    defaultValue: Set<T>,
    private readonly parser: (singleValue: string) => T
  ) {
    super(name, defaultValue);
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
