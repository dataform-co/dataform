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
