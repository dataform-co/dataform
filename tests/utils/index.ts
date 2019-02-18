export function asPlainObject<T>(object: T): T {
  return JSON.parse(JSON.stringify(object)) as T;
}

export function cleanSql(value: string) {
  var cleanValue = value;
  while (true) {
    let newCleanVal = cleanValue
      .replace("  ", " ")
      .replace("\t", " ")
      .replace("\n", " ")
      .replace("( ", "(")
      .replace(" )", ")");
    if (newCleanVal != cleanValue) {
      cleanValue = newCleanVal;
      continue;
    }
    return newCleanVal.toLowerCase().trim();
  }
}
