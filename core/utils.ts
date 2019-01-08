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
  const originalFunc = Error.prepareStackTrace;
  let callerfile;
  let lastfile;
  try {
    const err = new Error();
    let currentfile;
    Error.prepareStackTrace = function(err, stack) {
      return stack;
    };

    currentfile = (err.stack as any).shift().getFileName();
    while (err.stack.length) {
      callerfile = (err.stack as any).shift().getFileName();
      if (callerfile) {
        lastfile = callerfile;
      }
      if (
        currentfile !== callerfile &&
        callerfile.includes(rootDir) &&
        !callerfile.includes("node_modules") &&
        // We don't want to attribute files in includes/ to the caller files.
        (callerfile.includes("definitions/") || callerfile.includes("models/"))
      )
        break;
    }
  } catch (e) {}
  Error.prepareStackTrace = originalFunc;

  return relativePath(callerfile || lastfile, rootDir);
}
