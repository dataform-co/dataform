import * as utils from "./utils";
import { TableContext } from "./table";
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
    return compileTableSql(code, path);
  }
  return code;
}

export function compileTableSql(code: string, path: string) {
  var functionsBindings = getFunctionPropertyNames(TableContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  const parsedCode = getJSCode(code);

  return `const publish = global.publish || global.materialize; publish("${utils.baseFilename(
    path
  )}").query(ctx => {${functionsBindings.join(" ")} ${parsedCode} })`;
}

export function compileOperationSql(code: string, path: string) {
  const functionsBindings = getFunctionPropertyNames(OperationContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  const parsedCode = getJSCode(code);

  return `operate("${utils.baseFilename(path)}").queries(ctx => {${functionsBindings.join(
    "\n"
  )} ${parsedCode}.split("\\n---\\n"); })`;
}

export function compileAssertionSql(code: string, path: string) {
  const functionsBindings = getFunctionPropertyNames(AssertionContext.prototype).map(
    name => `const ${name} = !!ctx.${name} ? ctx.${name}.bind(ctx) : () => "";`
  );
  const parsedCode = getJSCode(code);

  return `assert("${utils.baseFilename(path)}").query(ctx => {${functionsBindings.join("\n")} ${parsedCode} })`;
}

export function extractJsBlocks(code: string): { sql: string; js: string } {
  const JS_REGEX = /\/\*[jJ][sS]\s*[\r\n]+((?:[^*]|[\r\n]|(?:\*+(?:[^*/]|[\r\n])))*)\*+\/|\-\-[jJ][sS]\s(.*)/g;
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

export function captureSingleBackticks(str: string): string {
  const RAW_BACKTICKS_REGEX = /([^\\])`/g;
  return str.replace(RAW_BACKTICKS_REGEX, (_, group1) => group1 + "\\`");
}

export function getJSCode(code: string) {
  const arr = code.split(/\n/);
  const result = [];
  let isJSBlock = false;
  let isCommentBlock = false;
  let isSQLBlock = false;

  arr.forEach((line, i) => {
    if (line.startsWith("/*js")) {
      isJSBlock = true;
      isSQLBlock = false;
      result.push(line.slice(4).trim());
    } else if (isJSBlock && !line.endsWith("*/")) {
      result.push(line.trim());
    } else if (isJSBlock && line.endsWith("*/")) {
      isJSBlock = false;
      result.push(
        line
          .trim()
          .slice(0, -2)
          .trim()
      );
    } else if (line.startsWith("/*")) {
      isCommentBlock = true;
      isSQLBlock = false;
      isJSBlock = false;
      result.push("");
    } else if (isCommentBlock) {
      isCommentBlock = !line.endsWith("*/");
      result.push("");
    } else if (line.startsWith("--js")) {
      isSQLBlock = false;
      result.push(line.slice(4).trim());
    } else if (
      (!isSQLBlock && (line.startsWith("--") || line.trim() === "")) ||
      (isSQLBlock && line.trim() === "" && arr.length - 1 === i)
    ) {
      result.push("");
    } else {
      if (!isSQLBlock) {
        isSQLBlock = true;
        isJSBlock = false;
      }
      const sql = captureSingleBackticks(line.trim());
      result.push(`sqlBlocks.push(\`${sql}\`);`);
    }
  });

  return " let sqlBlocks = []; " + result.join("\n") + ' return sqlBlocks.filter(item => item !== "").join("\\n")';
}

export function getFunctionPropertyNames(prototype: any) {
  return Object.getOwnPropertyNames(prototype).filter(function(e, i, arr) {
    if (e != arr[i + 1] && typeof prototype[e] == "function") return true;
  });
}
