import * as utils from "@dataform/core/utils";

export function compile(code: string, path: string) {
  if (path.endsWith(".assert.sql")) {
    return compileAssertionSql(code, path);
  }
  if (path.endsWith(".ops.sql")) {
    return compileOperationSql(code, path);
  }
  if (path.endsWith(".sql")) {
    return compileTableSql(code, path);
  }
  return code;
}

const tableContextPropertyNames = [
  "constructor",
  "config",
  "self",
  "ref",
  "resolve",
  "type",
  "where",
  "preOps",
  "postOps",
  "disabled",
  "redshift",
  "bigquery",
  "dependencies",
  "descriptor",
  "describe",
  "apply"
];

export function compileTableSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = tableContextPropertyNames.map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );

  return `
  const publish = global.publish || global.materialize;
  publish("${utils.baseFilename(path)}").query(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`;
  })`;
}

const operationContextPropertyNames = [
  "constructor",
  "self",
  "ref",
  "resolve",
  "dependencies",
  "hasOutput",
  "apply"
];

export function compileOperationSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = operationContextPropertyNames.map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );

  return `
  operate("${utils.baseFilename(path)}").queries(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`.split("\\n---\\n");
  })`;
}

const assertionContextPropertyNames = ["constructor", "ref", "resolve", "dependencies", "apply"];

export function compileAssertionSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = assertionContextPropertyNames.map(
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
  const JS_REGEX = /^\s*\/\*[jJ][sS]\s*[\r\n]+((?:[^*]|[\r\n]|(?:\*+(?:[^*/]|[\r\n])))*)\*+\/|^\s*\-\-[jJ][sS]\s(.*)/gm;
  // This captures any single backticks that aren't escaped with a preceding \.
  const RAW_BACKTICKS_REGEX = /([^\\])`/g;
  const jsBlocks: string[] = [];
  const cleanSql = code
    .replace(JS_REGEX, (_, group1, group2) => {
      if (group1) {
        jsBlocks.push(group1);
      }
      if (group2) {
        jsBlocks.push(group2);
      }
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
    if (e != arr[i + 1] && typeof prototype[e] == "function") {
      return true;
    }
  });
}
