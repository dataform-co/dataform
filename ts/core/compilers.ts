import * as utils from "./utils";

export function compile(code, path) {
  if (path.endsWith(".assert.sql")) {
    return compileAssertionSql(code, path);
  }
  if (path.endsWith(".ops.sql")) {
    return compileOperationSql(code, path);
  }
  if (path.endsWith(".sql")) {
    return compileMaterializationSql(code, path);
  }
  return code;
}

export function compileMaterializationSql(code: string, path: string) {
  var { sql, js } = extractJsBlocks(code);
  return `
  materialize("${utils.baseFilename(path)}").query(ctx => {
    const config = ctx.config.bind(ctx);
    const type = ctx.type.bind(ctx);
    const preOps = ctx.preOps.bind(ctx);
    const postOps = ctx.postOps.bind(ctx);
    const ref = ctx.ref.bind(ctx);
    const self = ctx.self.bind(ctx);
    const dependencies = ctx.dependencies.bind(ctx);
    const where = ctx.where.bind(ctx);
    const descriptor = ctx.descriptor.bind(ctx);
    const describe = ctx.describe.bind(ctx);
    ${js}
    return \`${sql}\`;
  })`;
}

export function compileOperationSql(code: string, path: string) {
  var { sql, js } = extractJsBlocks(code);
  return `
  operate("${utils.baseFilename(path)}").queries(ctx => {
    const ref = ctx.ref.bind(ctx);
    const dependencies = ctx.dependencies.bind(ctx);
    ${js}
    return \`${sql}\`.split("\\n---\\n");
  })`;
}

export function compileAssertionSql(code: string, path: string) {
  var { sql, js } = extractJsBlocks(code);
  return `
  assert("${utils.baseFilename(path)}").query(ctx => {
    const ref = ctx.ref.bind(ctx);
    const dependencies = ctx.dependencies.bind(ctx);
    ${js}
    return \`${sql}\`;
  })`;
}

export function extractJsBlocks(code: string): { sql: string; js: string } {
  const JS_REGEX = /\/\*[jJ][sS][\r\n]+((?:[^*]|[\r\n]|(?:\*+(?:[^*/]|[\r\n])))*)\*+\/|\-\-[jJ][sS]\s(.*)/g;
  var blocks: string[] = [];
  var cleanCode = code.replace(JS_REGEX, (_, group1, group2) => {
    if (group1) blocks.push(group1);
    if (group2) blocks.push(group2);
    return "";
  });
  return {
    sql: cleanCode.trim(),
    js: blocks.map(block => block.trim()).join("\n")
  };
}
