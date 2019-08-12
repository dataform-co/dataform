import * as moo from "moo";

const LEXER_STATE_NAMES = {
  SQL: "sql",
  JS_BLOCK: "jsBlock",
  JS_TEMPLATE_STRING: "jsTemplateString",
  INNER_SQL_BLOCK: "incrementalBlock"
};

const SQL_LEXER_TOKEN_NAMES = {
  START_CONFIG: LEXER_STATE_NAMES.SQL + "_startConfig",
  START_JS: LEXER_STATE_NAMES.SQL + "_startJs",
  START_INCREMENTAL: LEXER_STATE_NAMES.SQL + "_startIncremental",
  START_PRE_OPERATIONS: LEXER_STATE_NAMES.SQL + "_startPreOperations",
  START_POST_OPERATIONS: LEXER_STATE_NAMES.SQL + "_startPostOperations",
  START_INPUT: LEXER_STATE_NAMES.SQL + "_startInput",
  STATEMENT_SEPERATOR: LEXER_STATE_NAMES.SQL + "_statementSeparator",
  SINGLE_LINE_COMMENT: LEXER_STATE_NAMES.SQL + "_singleLineComment",
  MULTI_LINE_COMMENT: LEXER_STATE_NAMES.SQL + "_multiLineComment",
  SINGLE_QUOTE_STRING: LEXER_STATE_NAMES.SQL + "_singleQuoteString",
  DOUBLE_QUOTE_STRING: LEXER_STATE_NAMES.SQL + "_doubleQuoteString",
  START_JS_PLACEHOLDER: LEXER_STATE_NAMES.SQL + "_startJsPlaceholder",
  ESCAPED_BACKTICK: LEXER_STATE_NAMES.SQL + "_escapedBacktick",
  BACKTICK: LEXER_STATE_NAMES.SQL + "_backtick",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.SQL + "_captureEverythingElse"
};

const JS_BLOCK_LEXER_TOKEN_NAMES = {
  SINGLE_LINE_COMMENT: LEXER_STATE_NAMES.JS_BLOCK + "_singleLineComment",
  MULTI_LINE_COMMENT: LEXER_STATE_NAMES.JS_BLOCK + "_multiLineComment",
  SINGLE_QUOTE_STRING: LEXER_STATE_NAMES.JS_BLOCK + "_singleQuoteString",
  DOUBLE_QUOTE_STRING: LEXER_STATE_NAMES.JS_BLOCK + "_doubleQuoteString",
  START_JS_TEMPLATE_STRING: LEXER_STATE_NAMES.JS_BLOCK + "_startJsTemplateString",
  START_JS_BLOCK: LEXER_STATE_NAMES.JS_BLOCK + "_startJsBlock",
  CLOSE_BLOCK: LEXER_STATE_NAMES.JS_BLOCK + "_closeBlock",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.JS_BLOCK + "_captureEverythingElse"
};

const JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES = {
  ESCAPED_BACKSLASH: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_escapedBackslash",
  ESCAPED_BACKTICK: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_escapedBacktick",
  ESCAPED_DOLLAR_BRACE: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_escapedDollarBrace",
  START_JS_BLOCK: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_startJsBlock",
  CLOSE_STRING: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_closeString",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_captureEverythingElse"
};

const INNER_SQL_BLOCK_LEXER_TOKEN_NAMES = {
  STATEMENT_SEPERATOR: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_statementSeparator",
  SINGLE_LINE_COMMENT: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_singleLineComment",
  MULTI_LINE_COMMENT: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_multiLineComment",
  SINGLE_QUOTE_STRING: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_singleQuoteString",
  DOUBLE_QUOTE_STRING: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_doubleQuoteString",
  START_JS_BLOCK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startJsBlock",
  CLOSE_BLOCK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_closeBlock",
  ESCAPED_BACKTICK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_escapedBacktick",
  BACKTICK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_backtick",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_captureEverythingElse"
};

const lexer = moo.states(buildSqlxLexer());

export interface ISqlxParseResults {
  config: string;
  js: string;
  sql: string[];
  incremental: string;
  preOperations: string[];
  postOperations: string[];
  input: { [label: string]: string };
}

export function parseSqlx(code: string): ISqlxParseResults {
  const results: ISqlxParseResults = {
    config: "",
    js: "",
    sql: [""],
    incremental: "",
    preOperations: [""],
    postOperations: [""],
    input: {}
  };
  let currentInputLabel;
  const parseState = new SqlxParseState();
  lexer.reset(code);
  for (const token of lexer) {
    const previousState = parseState.currentState;
    const newState = parseState.computeState(token);

    if (token.type === SQL_LEXER_TOKEN_NAMES.START_INPUT) {
      currentInputLabel = token.value;
      token.value = "";
    }

    const isStatementSeparator =
      token.type === SQL_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR ||
      token.type === INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR;

    switch (newState) {
      case "config": {
        results.config += token.value;
        break;
      }
      case "js": {
        results.js += token.value;
        break;
      }
      case "sql": {
        if (isStatementSeparator) {
          results.sql.push("");
        }
        results.sql[results.sql.length - 1] += token.value;
        break;
      }
      case "incremental": {
        if (isStatementSeparator) {
          throw new Error(
            "Incremental code blocks may not contain SQL statement separators ('---')."
          );
        }
        results.incremental += token.value;
        break;
      }
      case "preOperations": {
        if (isStatementSeparator) {
          results.preOperations.push("");
        }
        results.preOperations[results.preOperations.length - 1] += token.value;
        break;
      }
      case "postOperations": {
        if (isStatementSeparator) {
          results.postOperations.push("");
        }
        results.postOperations[results.postOperations.length - 1] += token.value;
        break;
      }
      case "input": {
        if (isStatementSeparator) {
          throw new Error("Input code blocks may not contain SQL statement separators ('---').");
        }
        if (!results.input[currentInputLabel]) {
          results.input[currentInputLabel] = "";
        }
        results.input[currentInputLabel] += token.value;
        break;
      }
      default:
        throw new Error(`Unrecognized parse state: ${newState}`);
    }

    if (previousState === "js" && newState !== "js") {
      // If we're closing off a JS block, cut off the last closing brace.
      // We have to do this because we intentionally cut off the starting brace during lexing.
      // We can't keep them because the user's JS must not be run inside a scoped block.
      results.js = results.js.substring(0, results.js.length - 1);
    }
  }
  return results;
}

const tokenTypeStateMapping = new Map<string, keyof ISqlxParseResults>();
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_CONFIG, "config");
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_JS, "js");
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_INCREMENTAL, "incremental");
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_PRE_OPERATIONS, "preOperations");
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_POST_OPERATIONS, "postOperations");
tokenTypeStateMapping.set(SQL_LEXER_TOKEN_NAMES.START_INPUT, "input");

class SqlxParseState {
  public currentState: keyof ISqlxParseResults = "sql";

  public computeState(token: moo.Token): keyof ISqlxParseResults {
    if (!token.type.startsWith(LEXER_STATE_NAMES.SQL)) {
      return this.currentState;
    }
    this.currentState = "sql";
    if (tokenTypeStateMapping.has(token.type)) {
      this.currentState = tokenTypeStateMapping.get(token.type);
    }
    return this.currentState;
  }
}

function buildSqlxLexer(): { [x: string]: moo.Rules } {
  const sqlLexer: moo.Rules = {};
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_CONFIG] = {
    match: "config {",
    push: LEXER_STATE_NAMES.JS_BLOCK,
    value: () => "{"
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_JS] = {
    match: "js {",
    push: LEXER_STATE_NAMES.JS_BLOCK,
    value: () => ""
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_INCREMENTAL] = {
    match: "incremental_where {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK,
    value: () => ""
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_PRE_OPERATIONS] = {
    match: "pre_operations {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK,
    value: () => ""
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_POST_OPERATIONS] = {
    match: "post_operations {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK,
    value: () => ""
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_INPUT] = {
    match: /input "[a-zA-Z0-9_-]+" {/,
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK,
    value: (value: string) => value.split('"')[1]
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR] = {
    match: /[^\S\r\n]*---[^\S\r\n]*$/,
    value: () => ""
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT] = {
    match: /--.*?$/,
    value: (value: string) => value.replace(/`/g, "\\`").replace(/\${/g, "\\${")
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT] = {
    match: /\/\*[\s\S]*?\*\//,
    value: (value: string) => value.replace(/`/g, "\\`").replace(/\${/g, "\\${")
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.SINGLE_QUOTE_STRING] = /'(?:\\['\\]|[^\n'\\])*'/;
  sqlLexer[SQL_LEXER_TOKEN_NAMES.DOUBLE_QUOTE_STRING] = /"(?:\\["\\]|[^\n"\\])*"/;
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_JS_PLACEHOLDER] = {
    match: "${",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.ESCAPED_BACKTICK] = {
    match: "\\`"
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.BACKTICK] = {
    match: "`",
    value: () => "\\`"
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const jsBlockLexer: moo.Rules = {};
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT] = /\/\/.*?$/;
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT] = /\/\*[\s\S]*?\*\//;
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.SINGLE_QUOTE_STRING] = /'(?:\\['\\]|[^\n'\\])*'/;
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.DOUBLE_QUOTE_STRING] = /"(?:\\["\\]|[^\n"\\])*"/;
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.START_JS_TEMPLATE_STRING] = {
    match: "`",
    push: LEXER_STATE_NAMES.JS_TEMPLATE_STRING
  };
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.START_JS_BLOCK] = {
    match: "{",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK] = { match: "}", pop: 1 };
  jsBlockLexer[JS_BLOCK_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const jsTemplateStringLexer: moo.Rules = {};
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKSLASH] = /\\\\/;
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKTICK] = /\\`/;
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.ESCAPED_DOLLAR_BRACE] = /\\\${`/;
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.START_JS_BLOCK] = {
    match: "${",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.CLOSE_STRING] = { match: "`", pop: 1 };
  jsTemplateStringLexer[JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const innerSqlBlockLexer: moo.Rules = {};
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR] = {
    match: /[^\S\r\n]*---[^\S\r\n]*$/,
    value: () => ""
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT] = /--.*?$/;
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT] = /\/\*[\s\S]*?\*\//;
  innerSqlBlockLexer[
    INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.SINGLE_QUOTE_STRING
  ] = /'(?:\\['\\]|[^\n'\\])*'/;
  innerSqlBlockLexer[
    INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.DOUBLE_QUOTE_STRING
  ] = /"(?:\\["\\]|[^\n"\\])*"/;
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_JS_BLOCK] = {
    match: "${",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK] = {
    match: "}",
    pop: 1,
    value: () => ""
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.ESCAPED_BACKTICK] = {
    match: "\\`"
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.BACKTICK] = {
    match: "`",
    value: () => "\\`"
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const lexerStates: { [x: string]: moo.Rules } = {};
  lexerStates[LEXER_STATE_NAMES.SQL] = sqlLexer;
  lexerStates[LEXER_STATE_NAMES.JS_BLOCK] = jsBlockLexer;
  lexerStates[LEXER_STATE_NAMES.JS_TEMPLATE_STRING] = jsTemplateStringLexer;
  lexerStates[LEXER_STATE_NAMES.INNER_SQL_BLOCK] = innerSqlBlockLexer;

  return lexerStates;
}
