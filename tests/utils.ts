export function asPlainObject<T>(object: T): T {
  return JSON.parse(JSON.stringify(object)) as T;
}
