import { AssertionContext, SqlxAssertionContext } from "@dataform/core/assertion";
import { OperationContext, SqlxOperationContext } from "@dataform/core/operation";
import { ISqlxParseResults, parseSqlx } from "@dataform/core/sqlx_parser";
import { SqlxTableContext, TableContext } from "@dataform/core/table";
import * as utils from "@dataform/core/utils";

export function compile(code: string, path: string) {
  if (path.endsWith(".sqlx")) {
    return compileSqlx(parseSqlx(code), path);
  }
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

function compileTableSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(TableContext.prototype).map(
    name => `const ${name} = ctx.${name}.bind(ctx);`
  );

  return `
  publish("${utils.baseFilename(path)}").query(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`;
  })`;
}

function compileOperationSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(OperationContext.prototype).map(
    name => `const ${name} = ctx.${name}.bind(ctx);`
  );

  return `
  operate("${utils.baseFilename(path)}").queries(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`.split("\\n---\\n");
  })`;
}

function compileAssertionSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(AssertionContext.prototype).map(
    name => `const ${name} = ctx.${name}.bind(ctx);`
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

function compileSqlx(results: ISqlxParseResults, path: string) {
  return `
const parsedConfig = ${results.config || "{}"};
const finalConfig = {
  name: "${utils.baseFilename(path)}",
  type: "operations",
  dependencies: [],
  ...parsedConfig
}

const hasPreOperations = ${results.preOperations.length > 1 || results.preOperations[0] !== ""};
const hasPostOperations = ${results.postOperations.length > 1 || results.postOperations[0] !== ""};
const action = session.sqlxAction(finalConfig, ${
    results.sql.length
  }, hasPreOperations, hasPostOperations);

switch (finalConfig.type) {
  case "view":
  case "table":
  case "incremental":
  case "inline": {
    action.query(ctx => {
      ${getFunctionPropertyNames(SqlxTableContext.prototype)
        .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
        .join("\n")}
      ${results.js}
      if (finalConfig.type === "incremental") {
        action.where(\`${results.incremental}\`);
      }
      if (hasPreOperations) {
        const preOperations = [${results.preOperations.map(sql => `\`${sql}\``)}];
        action.preOps(preOperations);
      }
      if (hasPostOperations) {
        const postOperations = [${results.postOperations.map(sql => `\`${sql}\``)}];
        action.postOps(postOperations);
      }
      return \`${results.sql[0]}\`;
    });
    break;
  }
  case "assertion": {
    action.query(ctx => {
      ${getFunctionPropertyNames(SqlxAssertionContext.prototype)
        .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
        .join("\n")}
      ${results.js}
      return \`${results.sql[0]}\`;
    });
    break;
  }
  case "operations": {
    action.queries(ctx => {
      ${getFunctionPropertyNames(SqlxOperationContext.prototype)
        .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
        .join("\n")}
      ${results.js}
      const operations = [${results.sql.map(sql => `\`${sql}\``)}];
      return operations;
    });
    break;
  }
  default: {
    session.compileError(new Error(\`Unrecognized action type: \${finalConfig.type}\`));
    break;
  }
}`;
}

function getFunctionPropertyNames(prototype: any) {
  return [
    ...new Set(
      Object.getOwnPropertyNames(prototype).filter(propertyName => {
        if (typeof prototype[propertyName] !== "function") {
          return false;
        }
        if (propertyName === "constructor") {
          return false;
        }
        return true;
      })
    )
  ];
}
