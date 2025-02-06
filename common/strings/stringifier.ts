export interface IStringifier<T> {
  stringify: (value: T) => string;
  parse: (value: string) => T;
}

export type IJSONPrimitive = string | number | boolean | string[] | number[] | boolean[] | null;

export class JSONObjectStringifier<T> implements IStringifier<T> {
  public stringify(value: T) {
    // Sort the object keys.
    return JSON.stringify(
      Object.keys(value)
        .sort()
        .reduce((acc, curr) => ({ ...acc, [curr]: (value as any)[curr] }), {})
    );
  }

  public parse(value: string) {
    return JSON.parse(value) as T;
  }
}

export class StringifiedSet<T> implements Set<T> {
  get size() {
    return this.set.size;
  }

  get [Symbol.toStringTag]() {
    return StringifiedSet.name;
  }

  private set: Set<string>;

  constructor(
    private readonly stringifier: IStringifier<T>,
    // tslint:disable-next-line: array-type
    values?: readonly T[] | null
  ) {
    if (values) {
      this.set = new Set<string>(values.map(value => stringifier.stringify(value)));
    } else {
      this.set = new Set<string>();
    }
  }

  public add(value: T): this {
    this.set.add(this.stringifier.stringify(value));
    return this;
  }

  public clear(): void {
    return this.set.clear();
  }

  public delete(value: T): boolean {
    return this.set.delete(this.stringifier.stringify(value));
  }

  public forEach(callbackfn: (value: T, value2: T, set: Set<T>) => void, thisArg?: any): void {
    return this.set.forEach(
      (value, value2, _) =>
        callbackfn(this.stringifier.parse(value), this.stringifier.parse(value2), null),
      thisArg
    );
  }

  public has(value: T): boolean {
    return this.set.has(this.stringifier.stringify(value));
  }

  public [Symbol.iterator](): IterableIterator<T> {
    return this.keys();
  }

  public entries(): IterableIterator<[T, T]> {
    const stringifier = this.stringifier;
    const innerIterator = this.set[Symbol.iterator]();
    return new (class Iter implements IterableIterator<[T, T]> {
      public [Symbol.iterator](): IterableIterator<[T, T]> {
        return this;
      }
      public next(): IteratorResult<[T, T], any> {
        const next = innerIterator.next();
        return {
          value: !next.done ? [stringifier.parse(next.value[0]), next.value[1]] : undefined,
          done: next.done
        };
      }
    })();
  }

  public keys(): IterableIterator<T> {
    const stringifier = this.stringifier;
    const innerIterator = this.set.keys();
    return new (class Iter implements IterableIterator<T> {
      public [Symbol.iterator](): IterableIterator<T> {
        return this;
      }
      public next(): IteratorResult<T> {
        const next = innerIterator.next();
        return {
          value: !next.done ? stringifier.parse(next.value) : undefined,
          done: next.done
        };
      }
    })();
  }

  public values(): IterableIterator<T> {
    const stringifier = this.stringifier;
    const innerIterator = this.set.values();
    return new (class Iter implements IterableIterator<T> {
      public [Symbol.iterator](): IterableIterator<T> {
        return this;
      }
      public next(): IteratorResult<T> {
        const next = innerIterator.next();
        return {
          value: !next.done ? stringifier.parse(next.value) : undefined,
          done: next.done
        };
      }
    })();
  }
}
