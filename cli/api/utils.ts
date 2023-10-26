export function prettyJsonStringify(obj: object) {
  return JSON.stringify(obj, null, 4) + "\n";
}
