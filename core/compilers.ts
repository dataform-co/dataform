import * as utils from "./utils";
import { MaterializationContext } from "./materialization";
import { OperationContext } from "./operation";
import { AssertionContext } from "./assertion";

export function compile(code: string, path: string) {
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
  var functionsBindings = getFunctionPropertyNames(MaterializationContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  return `
  materialize("${utils.baseFilename(path)}").query(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`;
  })`;
}

export function compileOperationSql(code: string, path: string) {
  var { sql, js } = extractJsBlocks(code);
  var functionsBindings = getFunctionPropertyNames(OperationContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  return `
  operate("${utils.baseFilename(path)}").queries(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`.split("\\n---\\n");
  })`;
}

export function compileAssertionSql(code: string, path: string) {
  var { sql, js } = extractJsBlocks(code);
  var functionsBindings = getFunctionPropertyNames(AssertionContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  return `
  assert("${utils.baseFilename(path)}").query(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`;
  })`;
}

export function extractJsBlocks(code: string): { sql: string; js: string } {
  const JS_REGEX = /\/\*[jJ][sS][\r\n]+((?:[^*]|[\r\n]|(?:\*+(?:[^*/]|[\r\n])))*)\*+\/|\-\-[jJ][sS]\s(.*)/g;
  // This captures any single backticks that aren't escaped with a preceding \.
  const RAW_BACKTICKS_REGEX = /([^\\])`/g;
  var jsBlocks: string[] = [];
  var cleanSql = code
    .replace(JS_REGEX, (_, group1, group2) => {
      if (group1) jsBlocks.push(group1);
      if (group2) jsBlocks.push(group2);
      return "";
    })
    .replace(RAW_BACKTICKS_REGEX, (_, group1) => group1 + "\\`");
  return {
    sql: cleanSql.trim(),
    js: jsBlocks.map(block => block.trim()).join("\n")
  };
}

export function getFunctionPropertyNames(prototype: any) {
  return Object.getOwnPropertyNames(prototype).filter(function(e, i, arr) {
    if (e != arr[i + 1] && typeof prototype[e] == "function") return true;
  });
}
