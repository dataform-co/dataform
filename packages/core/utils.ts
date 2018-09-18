export function baseFilename(path: string) {
  var pathSplits = path.split("/");
  return pathSplits[pathSplits.length - 1].split(".")[0];
}

export function compileSql(code: string, path: string) {
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

export function variableNameFriendly(value: string) {
  return value
    .replace("-", "");
}
