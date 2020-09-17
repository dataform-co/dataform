import Long from "long";

export interface IStringifier<T> {
  stringify: (value: T) => string;
  parse: (value: string) => T;
}

export type IJSONPrimitive = string | number | boolean | string[] | number[] | boolean[] | null;

export class JSONStringifier<T extends IJSONPrimitive> implements IStringifier<T> {
  public static create<T extends IJSONPrimitive>() {
    return new JSONStringifier<T>();
  }

  public stringify(value: T) {
    return JSON.stringify(value);
  }

  public parse(value: string) {
    return JSON.parse(value) as T;
  }
}

export class JSONObjectStringifier<T> implements IStringifier<T> {
  public static create<T>() {
    return new JSONObjectStringifier<T>();
  }

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

export class LongStringifier implements IStringifier<Long> {
  public static create() {
    return new LongStringifier();
  }

  public stringify(value: Long) {
    return value.toString();
  }

  public parse(value: string) {
    return Long.fromString(value);
  }
}

export class ArrayStringifier<T> implements IStringifier<T[]> {
  public static create<T>(stringifier: IStringifier<T>) {
    return new ArrayStringifier(stringifier);
  }

  constructor(private stringifier: IStringifier<T>) {}

  public stringify(value: T[]) {
    return JSON.stringify(value.map(v => this.stringifier.stringify(v)));
  }

  public parse(value: string) {
    return (JSON.parse(value) as string[]).map(v => this.stringifier.parse(v));
  }
}

// Stringifying a string doesn't require any operation; like the empty set, or an empty monad.
export class StringStringifier implements IStringifier<string> {
  public static create(stringifier: IStringifier<string>) {
    return new StringStringifier(stringifier);
  }

  constructor(private stringifier: IStringifier<string>) {}

  public stringify(value: string) {
    return value;
  }

  public parse(value: string) {
    return value;
  }
}

export class StringifiedMap<K, V> implements Map<K, V> {
  get [Symbol.toStringTag]() {
    return StringifiedMap.name;
  }

  get size() {
    return this.map.size;
  }

  private map: Map<string, V>;

  constructor(
    private readonly stringifier: IStringifier<K>,
    // tslint:disable-next-line: array-type
    entries?: readonly (readonly [K, V])[] | null
  ) {
    if (entries) {
      this.map = new Map<string, V>(
        entries.map(([key, value]) => [stringifier.stringify(key), value])
      );
    } else {
      this.map = new Map<string, V>();
    }
  }

  public clear(): void {
    return this.map.clear();
  }

  public delete(key: K): boolean {
    return this.map.delete(this.stringifier.stringify(key));
  }

  public forEach(callbackfn: (value: V, key: K, map: Map<K, V>) => void, thisArg?: any): void {
    return this.map.forEach(
      (value, key, _) => callbackfn(value, this.stringifier.parse(key), null),
      thisArg
    );
  }

  public get(key: K): V {
    return this.map.get(this.stringifier.stringify(key));
  }

  public has(key: K): boolean {
    return this.map.has(this.stringifier.stringify(key));
  }

  public set(key: K, value: V): this {
    this.map.set(this.stringifier.stringify(key), value);
    return this;
  }

  public [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.entries();
  }

  public entries(): IterableIterator<[K, V]> {
    const stringifier = this.stringifier;
    const innerIterator = this.map[Symbol.iterator]();
    return new (class Iter implements IterableIterator<[K, V]> {
      public [Symbol.iterator](): IterableIterator<[K, V]> {
        return this;
      }
      public next(): IteratorResult<[K, V], any> {
        const next = innerIterator.next();
        return {
          value: !next.done ? [stringifier.parse(next.value[0]), next.value[1]] : undefined,
          done: next.done
        };
      }
    })();
  }
  public keys(): IterableIterator<K> {
    const stringifier = this.stringifier;
    const innerIterator = this.map.keys();
    return new (class Iter implements IterableIterator<K> {
      public [Symbol.iterator](): IterableIterator<K> {
        return this;
      }
      public next(): IteratorResult<K, any> {
        const next = innerIterator.next();
        return {
          value: !next.done ? stringifier.parse(next.value) : undefined,
          done: next.done
        };
      }
    })();
  }
  public values(): IterableIterator<V> {
    return this.map.values();
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
