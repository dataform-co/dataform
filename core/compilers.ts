import { AssertionContext } from "@dataform/core/assertion";
import { OperationContext } from "@dataform/core/operation";
import { TableContext } from "@dataform/core/table";
import * as utils from "@dataform/core/utils";
import * as moo from "moo";

// NOTES:
// "example_backticks.sqlx": different than previous syntax, we require an escape backslash before the first backtick.
// "example_incremental.sqlx", "example_inline.sql": assertion had to be changed to trim() whitespace off the query (IMO keeping whitespace is important for sourcemaps!).
// previous "override_schema.example.sql": doesn't actually work any more, that way of overriding schemas was removed (?), wasn't even tested, I've deleted it.
// if somebody wants to call a function inside a config block, they'll have to do this: config { ...myConfigFunction() }

const lexer = moo.states({
  sql: {
    sql_start_config: { match: "config {", push: "js_block", value: () => "{" },
    sql_start_js: { match: "js {", push: "js_block", value: () => "" },
    sql_start_incremental: {
      match: "if_incremental {",
      push: "incremental_block",
      value: () => ""
    },
    sql_single_line_comment: /--.*?$/,
    sql_multi_line_comment: /\/\*[\s\S]*?\*\//,
    sql_single_quote_string: /'(?:\\['\\]|[^\n'\\])*'/,
    sql_double_quote_string: /"(?:\\["\\]|[^\n"\\])*"/,
    sql_start_new_block: { match: "${", push: "js_block" },
    sql_everything_else: { match: /[\s\S]+?/, lineBreaks: true }
  },
  js_block: {
    js_block_single_line_comment: /\/\/.*?$/,
    js_block_multi_line_comment: /\/\*[\s\S]*?\*\//,
    js_block_single_quote_string: /'(?:\\['\\]|[^\n'\\])*'/,
    js_block_double_quote_string: /"(?:\\["\\]|[^\n"\\])*"/,
    js_block_start_js_template_string: { match: "`", push: "js_template_string" },
    js_block_start_new_block: { match: "{", push: "js_block" },
    js_block_stop_block: { match: "}", pop: 1 },
    js_block_everything_else: { match: /[\s\S]+?/, lineBreaks: true }
  },
  js_template_string: {
    js_template_string_escaped_backslash: /\\\\/,
    js_template_string_escaped_backtick: /\\`/,
    js_template_string_escaped_dollarbrace: /\\\${`/,
    js_template_string_start_new_block: { match: "${", push: "js_block" },
    js_template_string_stop_string: { match: "`", pop: 1 },
    js_template_string_everything_else: { match: /[\s\S]+?/, lineBreaks: true }
  },
  incremental_block: {
    incremental_single_line_comment: /--.*?$/,
    incremental_multi_line_comment: /\/\*[\s\S]*?\*\//,
    incremental_single_quote_string: /'(?:\\['\\]|[^\n'\\])*'/,
    incremental_double_quote_string: /"(?:\\["\\]|[^\n"\\])*"/,
    incremental_start_new_block: { match: "${", push: "js_block" },
    incremental_stop_block: { match: "}", pop: 1, value: () => "" },
    incremental_everything_else: { match: /[\s\S]+?/, lineBreaks: true }
  }
});

export function compile(code: string, path: string) {
  if (path.endsWith(".sqlx")) {
    const results = parseSqlx(code, path);
    const result = `
    const DEFAULT_CONFIG = {
      // TODO: switch this default to "operations"
      type: "view",
      dependencies: [],
      schemaOverride: ""
    };
    const userConfig = ${results.config || "{}"};
    const finalConfig = {
      ...DEFAULT_CONFIG,
      ...userConfig
    };

    switch (finalConfig.type) {
      case "view":
      case "table":
      case "incremental":
      case "inline": {
        const dataset = publish("${utils.baseFilename(path)}");
        if (finalConfig.schemaOverride) {
          // TODO: this is quite the hack... 
          dataset.proto.target.schema = finalConfig.schemaOverride;
        }
        dataset.query(ctx => {
          // TODO: this should really be pulled out before this switch statement. that might be tricky though!
          ctx.type(finalConfig.type);
          ctx.dependencies(finalConfig.dependencies);
          if (finalConfig.type === "incremental") {
            ctx.where("${results.incremental.replace(/\n/g, "\\\n")}");
          }

          // TODO: remove methods from TableContext which the user should not be allowed to call, e.g. dependencies().
          // will that even be possible, given that JS needs to be able to call this too?
          // answering myself: yes, we just need to do it only *here*, as this user code JS should definitely not be calling such methods.
          ${getFunctionPropertyNames(TableContext.prototype)
            .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
            .join("\n")}
          ${results.js}
          return \`${results.sql}\`;
        });
        break;
      }
      case "assertion": {
        const assertion = assert("${utils.baseFilename(path)}");
        if (finalConfig.schemaOverride) {
          // TODO: this is quite the hack... 
          assertion.proto.target.schema = finalConfig.schemaOverride;
        }
        assertion.query(ctx => {
          // TODO: this should really be pulled out before this switch statement. that might be tricky though!
          ctx.dependencies(finalConfig.dependencies);

          // TODO: remove methods from AssertionContext which the user should not be allowed to call, e.g. dependencies().
          // will that even be possible, given that JS needs to be able to call this too?
          // answering myself: yes, we just need to do it only *here*, as this user code JS should definitely not be calling such methods.
          ${getFunctionPropertyNames(AssertionContext.prototype)
            .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
            .join("\n")}
          ${results.js}
          return \`${results.sql}\`;
        });
        break;
      }
      default:
        throw new Error(\`Unrecognized action type: \${finalConfig.type}\`);
    }
     `;
    return result;
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

export function compileTableSql(code: string, path: string) {
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

export function compileOperationSql(code: string, path: string) {
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

export function compileAssertionSql(code: string, path: string) {
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

interface ISqlxParseResults {
  config: string;
  js: string;
  sql: string;
  incremental: string;
}

class SqlxParseState {
  private currentState: keyof ISqlxParseResults = "sql";

  public computeState(token: moo.Token): keyof ISqlxParseResults {
    if (!token.type.startsWith("sql")) {
      return this.currentState;
    }
    switch (token.type) {
      case "sql_start_config": {
        this.currentState = "config";
        break;
      }
      case "sql_start_js": {
        this.currentState = "js";
        break;
      }
      case "sql_start_incremental": {
        this.currentState = "incremental";
        break;
      }
      default: {
        this.currentState = "sql";
        break;
      }
    }
    return this.currentState;
  }
}

function parseSqlx(code: string, path: string): ISqlxParseResults {
  lexer.reset(code);
  const parseState = new SqlxParseState();
  const results = { config: "", js: "", sql: "", incremental: "" };
  for (const token of lexer) {
    results[parseState.computeState(token)] += token.value;
  }
  if (results.js) {
    // If the user provided any JS, cut off the last closing brace.
    // (We have to do this so that JS scoping works, and because we intentionally cut off the starting brace during lexing.)
    results.js = results.js.substring(0, results.js.length - 1);
  }
  return results;
}
