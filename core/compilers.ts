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
    const str = line.trim();

    if (str.toLowerCase().startsWith("/*js")) {
      isJSBlock = true;
      isSQLBlock = false;
      result.push(str.slice(4).trim());
    } else if (isJSBlock && !str.endsWith("*/")) {
      result.push(str);
    } else if (isJSBlock && str.endsWith("*/")) {
      isJSBlock = false;
      result.push(str.slice(0, -2).trim());
    } else if (str.startsWith("/*")) {
      isCommentBlock = true;
      isSQLBlock = false;
      isJSBlock = false;
      result.push("");
    } else if (isCommentBlock) {
      isCommentBlock = !str.endsWith("*/");
      result.push("");
    } else if (str.toLowerCase().startsWith("--js")) {
      isSQLBlock = false;
      result.push(str.slice(4).trim());
    } else if (
      (!isSQLBlock && (str.startsWith("--") || str === "")) ||
      (isSQLBlock && str === "" && arr.length - 1 === i)
    ) {
      result.push("");
    } else {
      if (!isSQLBlock) {
        isSQLBlock = true;
        isJSBlock = false;
      }
      const sql = captureSingleBackticks(str);
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
