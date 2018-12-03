export function relativePath(path: string, base: string) {
  if (base.length == 0) {
    return path;
  }
  var stripped = path.substr(base.length);
  if (stripped.startsWith("/")) {
    return stripped.substr(1);
  } else {
    return stripped;
  }
}

export function baseFilename(path: string) {
  var pathSplits = path.split("/");
  return pathSplits[pathSplits.length - 1].split(".")[0];
}

export function variableNameFriendly(value: string) {
  return value
    .replace("-", "")
    .replace("@", "")
    .replace("/", "");
}

export function matchPatterns(patterns: string[], values: string[]) {
  var regexps = patterns.map(
    pattern =>
      new RegExp(
        "^" +
          pattern
            .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
            .split("*")
            .join(".*") +
          "$"
      )
  );
  return values.filter(value => regexps.filter(regexp => regexp.test(value)).length > 0);
}

export function getCallerFile(rootDir: string) {
  var originalFunc = Error.prepareStackTrace;
  var callerfile;
  try {
    var err = new Error();
    var currentfile;
    Error.prepareStackTrace = function(err, stack) {
      return stack;
    };
    currentfile = (err.stack as any).shift().getFileName();
    while (err.stack.length) {
      callerfile = (err.stack as any).shift().getFileName();

      if (
        currentfile !== callerfile &&
        !callerfile.includes("vm2/lib/") &&
        !callerfile.includes("@dataform/core/") &&
        callerfile.startsWith(rootDir)
      )
        break;
    }
  } catch (e) {}
  Error.prepareStackTrace = originalFunc;
  return relativePath(callerfile, rootDir);
}
