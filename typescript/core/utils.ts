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

export function compileMaterializationSql(code: string, path: string) {
  return `
  materialize("${baseFilename(path)}").query(ctx => {
    const type = ctx.type.bind(ctx);
    const post = ctx.post.bind(ctx);
    const pre = ctx.pre.bind(ctx);
    const ref = ctx.ref.bind(ctx);
    const self = ctx.self.bind(ctx);
    const dependency = ctx.dependency.bind(ctx);
    const where = ctx.where.bind(ctx);
    const describe = ctx.describe.bind(ctx);
    const assert = ctx.assert.bind(ctx);
    return \`${code}\`;
  })`;
}

export function compileOperationSql(code: string, path: string) {
  return `
  operate("${baseFilename(path)}").statement(ctx => {
    const ref = ctx.ref.bind(ctx);
    const dependency = ctx.dependency.bind(ctx);
    return \`${code}\`.split("\\n---\\n");
  })`;
}

export function compileAssertionSql(code: string, path: string) {
  return `
  assert("${baseFilename(path)}").query(ctx => {
    const ref = ctx.ref.bind(ctx);
    const dependency = ctx.dependency.bind(ctx);
    return \`${code}\`.split("\\n---\\n");
  })`;
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
  return values.filter(
    value => regexps.filter(regexp => regexp.test(value)).length > 0
  );
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

      if (currentfile !== callerfile && !callerfile.includes("vm2/lib/") && ! callerfile.includes("@dataform/core/")) break;
    }
  } catch (e) {}
  Error.prepareStackTrace = originalFunc;
  return relativePath(callerfile, rootDir);
}
