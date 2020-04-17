import * as moo from "moo";

const LEXER_STATE_NAMES = {
  SQL: "sql",
  JS_BLOCK: "jsBlock",
  JS_TEMPLATE_STRING: "jsTemplateString",
  INNER_SQL_BLOCK: "innerSqlBlock",
  SQL_SINGLE_QUOTE_STRING: "innerSingleQuote",
  SQL_DOUBLE_QUOTE_STRING: "innerDoubleQuote"
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
  START_JS_PLACEHOLDER: LEXER_STATE_NAMES.SQL + "_startJsPlaceholder",
  BACKSLASH: LEXER_STATE_NAMES.SQL + "_backslash",
  BACKTICK: LEXER_STATE_NAMES.SQL + "_backtick",
  START_QUOTE_SINGLE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startQuoteSingle",
  START_QUOTE_DOUBLE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startQuoteDouble",
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
  ESCAPED_DOLLAR_BRACE: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_escapedDollarBrace",
  START_JS_BLOCK: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_startJsBlock",
  CLOSE_STRING: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_closeString",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.JS_TEMPLATE_STRING + "_captureEverythingElse"
};

const INNER_SQL_BLOCK_LEXER_TOKEN_NAMES = {
  STATEMENT_SEPERATOR: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_statementSeparator",
  SINGLE_LINE_COMMENT: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_singleLineComment",
  MULTI_LINE_COMMENT: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_multiLineComment",
  START_JS_PLACEHOLDER: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startJsPlaceholder",
  CLOSE_BLOCK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_closeBlock",
  BACKSLASH: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_backslash",
  BACKTICK: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_backtick",
  START_QUOTE_SINGLE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startQuoteSingle",
  START_QUOTE_DOUBLE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_startQuoteDouble",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.INNER_SQL_BLOCK + "_captureEverythingElse"
};

const SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES = {
  ESCAPED_BACKSLASH: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING + "_escapedBackslash",
  ESCAPED_QUOTE: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING + "_escapedQuoteSingle",
  BACKSLASH: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING + "_backslash",
  CLOSE_QUOTE: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING + "_closeQuoteSingle",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING + "_captureEverythingElse"
};

const SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES = {
  ESCAPED_BACKSLASH: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING + "_escapedBackslash",
  ESCAPED_QUOTE: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING + "_escapedQuoteDouble",
  BACKSLASH: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING + "_backslash",
  CLOSE_QUOTE: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING + "_closeQuoteDouble",
  CAPTURE_EVERYTHING_ELSE: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING + "_captureEverythingElse"
};

const lexer = moo.states(buildSqlxLexer());

export enum SyntaxTreeNodeType {
  JAVASCRIPT,
  JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER,
  SQL,
  SQL_COMMENT,
  SQL_LITERAL_STRING,
  SQL_STATEMENT_SEPARATOR
}

const START_TOKEN_NODE_MAPPINGS = new Map<string, SyntaxTreeNodeType>([
  [SQL_LEXER_TOKEN_NAMES.START_CONFIG, SyntaxTreeNodeType.JAVASCRIPT],
  [SQL_LEXER_TOKEN_NAMES.START_JS, SyntaxTreeNodeType.JAVASCRIPT],
  [JS_BLOCK_LEXER_TOKEN_NAMES.START_JS_TEMPLATE_STRING, SyntaxTreeNodeType.JAVASCRIPT],
  [JS_BLOCK_LEXER_TOKEN_NAMES.START_JS_BLOCK, SyntaxTreeNodeType.JAVASCRIPT],
  [JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.START_JS_BLOCK, SyntaxTreeNodeType.JAVASCRIPT],

  [
    SQL_LEXER_TOKEN_NAMES.START_JS_PLACEHOLDER,
    SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER
  ],
  [
    INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_JS_PLACEHOLDER,
    SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER
  ],

  [SQL_LEXER_TOKEN_NAMES.START_INCREMENTAL, SyntaxTreeNodeType.SQL],
  [SQL_LEXER_TOKEN_NAMES.START_PRE_OPERATIONS, SyntaxTreeNodeType.SQL],
  [SQL_LEXER_TOKEN_NAMES.START_POST_OPERATIONS, SyntaxTreeNodeType.SQL],
  [SQL_LEXER_TOKEN_NAMES.START_INPUT, SyntaxTreeNodeType.SQL],

  [SQL_LEXER_TOKEN_NAMES.START_QUOTE_SINGLE, SyntaxTreeNodeType.SQL_LITERAL_STRING],
  [SQL_LEXER_TOKEN_NAMES.START_QUOTE_DOUBLE, SyntaxTreeNodeType.SQL_LITERAL_STRING],
  [INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_QUOTE_SINGLE, SyntaxTreeNodeType.SQL_LITERAL_STRING],
  [INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_QUOTE_DOUBLE, SyntaxTreeNodeType.SQL_LITERAL_STRING]
]);

const CLOSE_TOKEN_TYPES = new Set<string>([
  JS_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK,
  JS_TEMPLATE_STRING_LEXER_TOKEN_NAMES.CLOSE_STRING,
  INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK,
  SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CLOSE_QUOTE,
  SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CLOSE_QUOTE
]);

const WHOLE_TOKEN_NODE_MAPPINGS = new Map<string, SyntaxTreeNodeType>([
  [SQL_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT, SyntaxTreeNodeType.SQL_COMMENT],
  [SQL_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT, SyntaxTreeNodeType.SQL_COMMENT],
  [INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT, SyntaxTreeNodeType.SQL_COMMENT],
  [INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT, SyntaxTreeNodeType.SQL_COMMENT],

  [SQL_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR, SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR],
  [
    INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR,
    SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR
  ]
]);

export class SyntaxTreeNode {
  public static create(code: string) {
    const parentNode = new SyntaxTreeNode(SyntaxTreeNodeType.SQL);
    let currentNode = parentNode;
    const nodeStack = [currentNode];
    lexer.reset(code);
    for (const token of lexer) {
      if (START_TOKEN_NODE_MAPPINGS.has(token.type)) {
        const childType = START_TOKEN_NODE_MAPPINGS.get(token.type);
        if (childType === SyntaxTreeNodeType.SQL && currentNode.type !== SyntaxTreeNodeType.SQL) {
          throw new Error("SQL syntax tree nodes may only be children of other SQL nodes.");
        }
        const newCurrentNode = new SyntaxTreeNode(childType, [token.value]);
        nodeStack.push(newCurrentNode);
        currentNode.push(newCurrentNode);
        currentNode = newCurrentNode;
      } else if (CLOSE_TOKEN_TYPES.has(token.type)) {
        currentNode.push(token.value);
        nodeStack.pop();
        currentNode = nodeStack[nodeStack.length - 1];
      } else if (WHOLE_TOKEN_NODE_MAPPINGS.has(token.type)) {
        currentNode.push(
          new SyntaxTreeNode(WHOLE_TOKEN_NODE_MAPPINGS.get(token.type)).push(token.value)
        );
      } else {
        currentNode.push(token.value);
      }
    }
    return parentNode;
  }

  public static isSyntaxTreeNode(node: string | SyntaxTreeNode): node is SyntaxTreeNode {
    return typeof node !== "string";
  }

  public constructor(
    public readonly type: SyntaxTreeNodeType,
    private allChildren: Array<string | SyntaxTreeNode> = []
  ) {}

  public children() {
    return this.allChildren.slice();
  }

  public concatenate(): string {
    return this.allChildren
      .map(child => {
        if (typeof child === "string") {
          return child;
        }
        return child.concatenate();
      })
      .join("");
  }

  public push(child: string | SyntaxTreeNode): this {
    if (
      this.allChildren.length > 0 &&
      typeof child === "string" &&
      typeof this.allChildren[this.allChildren.length - 1] === "string"
    ) {
      this.allChildren[this.allChildren.length - 1] =
        this.allChildren[this.allChildren.length - 1] + child;
      return;
    }
    this.allChildren.push(child);
    return this;
  }

  public equals(other: SyntaxTreeNode): boolean {
    if (this.type !== other.type) {
      return false;
    }
    if (this.allChildren.length !== other.children().length) {
      return false;
    }

    const areEqual = (first: string | SyntaxTreeNode, second: string | SyntaxTreeNode) => {
      if (typeof first !== typeof second) {
        return false;
      }
      if (typeof first === "string" || typeof second === "string") {
        return first === second;
      }
      return first.equals(second);
    };
    if (this.allChildren.some((child, index) => !areEqual(child, other.children()[index]))) {
      return false;
    }
    return true;
  }
}

export interface ISqlxParseResults {
  config: string;
  js: string;
  sql: string[];
  incremental: string;
  preOperations: string[];
  postOperations: string[];
  input: { [label: string]: string };
}

function escapeSqlxNodeForJavaScriptTemplateString(node: string | SyntaxTreeNode) {
  if (typeof node === "string") {
    return node.replace(/\\/g, "\\\\").replace(/\`/g, "\\`");
  }
  switch (node.type) {
    case SyntaxTreeNodeType.SQL_COMMENT:
      // Any code (i.e. JavaScript placeholder strings) inside comments should not run, so escape it.
      return node
        .concatenate()
        .replace(/`/g, "\\`")
        .replace(/\${/g, "\\${");
    case SyntaxTreeNodeType.SQL_LITERAL_STRING:
      // Literal strings may contain escapes, which we need to double-escape.
      return node.concatenate().replace(/\\/g, "\\\\");
  }
  return node;
}

export function parseSqlx(code: string): ISqlxParseResults {
  const valueMappings = getValueMappings();
  const results: ISqlxParseResults = {
    config: "",
    js: "",
    sql: [""],
    incremental: "",
    preOperations: [""],
    postOperations: [""],
    input: {}
  };

  const rootNode = SyntaxTreeNode.create(code);

  rootNode
    .children()
    .filter(SyntaxTreeNode.isSyntaxTreeNode)
    .filter(node => node.type === SyntaxTreeNodeType.JAVASCRIPT)
    .forEach(node => {
      const concatenated = node.concatenate();
      if (concatenated.startsWith("config")) {
        results.config = concatenated.slice("config ".length);
      } else {
        results.js += concatenated.slice("js {".length, "}".length * -1);
      }
    });

  rootNode
    .children()
    .filter(
      node =>
        typeof node === "string" ||
        [
          SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER,
          SyntaxTreeNodeType.SQL_COMMENT,
          SyntaxTreeNodeType.SQL_LITERAL_STRING,
          SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR
        ].includes(node.type)
    )
    .map(escapeSqlxNodeForJavaScriptTemplateString)
    .forEach(node => {
      if (typeof node !== "string" && node.type === SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR) {
        results.sql.push("");
        return;
      }
      results.sql[results.sql.length - 1] += typeof node === "string" ? node : node.concatenate();
    });

  let currentInputLabel;
  const parseState = new SqlxParseState();
  lexer.reset(code);
  for (const token of lexer) {
    if (valueMappings[token.type]) {
      token.value = valueMappings[token.type](token.value);
    }
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
        break;
      }
      case "js": {
        break;
      }
      case "sql": {
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

function getValueMappings() {
  const valueMappings: {
    [tokenType: string]: (tokenValue: string) => string;
  } = {};

  valueMappings[SQL_LEXER_TOKEN_NAMES.START_INCREMENTAL] = () => "";
  valueMappings[SQL_LEXER_TOKEN_NAMES.START_PRE_OPERATIONS] = () => "";
  valueMappings[SQL_LEXER_TOKEN_NAMES.START_POST_OPERATIONS] = () => "";
  valueMappings[SQL_LEXER_TOKEN_NAMES.START_INPUT] = (tokenValue: string) =>
    tokenValue.split('"')[1];

  valueMappings[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR] = () => "";
  valueMappings[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK] = () => "";
  valueMappings[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.BACKTICK] = () => "\\`";
  valueMappings[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.BACKSLASH] = () => "\\\\";

  valueMappings[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKSLASH] = () => "\\\\\\\\";
  valueMappings[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_QUOTE] = () => "\\\\'";
  valueMappings[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = (
    value: string
  ) => value.replace(/\\/g, "\\\\");

  valueMappings[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKSLASH] = () => "\\\\\\\\";
  valueMappings[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_QUOTE] = () => '\\\\"';
  valueMappings[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = (
    value: string
  ) => value.replace(/\\/g, "\\\\");

  return valueMappings;
}

function buildSqlxLexer(): { [x: string]: moo.Rules } {
  const sqlLexer: moo.Rules = {};
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_CONFIG] = {
    match: "config {",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_JS] = {
    match: "js {",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_INCREMENTAL] = {
    match: "incremental_where {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_PRE_OPERATIONS] = {
    match: "pre_operations {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_POST_OPERATIONS] = {
    match: "post_operations {",
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_INPUT] = {
    match: /input "[a-zA-Z0-9_-]+" {/,
    push: LEXER_STATE_NAMES.INNER_SQL_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR] = /[^\S\r\n]*---[^\S\r\n]*$/;
  sqlLexer[SQL_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT] = /--.*?$/;
  sqlLexer[SQL_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT] = /\/\*[\s\S]*?\*\//;
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_JS_PLACEHOLDER] = {
    match: "${",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.BACKSLASH] = "\\";
  sqlLexer[SQL_LEXER_TOKEN_NAMES.BACKTICK] = "`";
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_QUOTE_SINGLE] = {
    match: "'",
    push: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING
  };
  sqlLexer[SQL_LEXER_TOKEN_NAMES.START_QUOTE_DOUBLE] = {
    match: '"',
    push: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING
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
  innerSqlBlockLexer[
    INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.STATEMENT_SEPERATOR
  ] = /[^\S\r\n]*---[^\S\r\n]*$/;
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.SINGLE_LINE_COMMENT] = /--.*?$/;
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.MULTI_LINE_COMMENT] = /\/\*[\s\S]*?\*\//;
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_JS_PLACEHOLDER] = {
    match: "${",
    push: LEXER_STATE_NAMES.JS_BLOCK
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CLOSE_BLOCK] = {
    match: "}",
    pop: 1
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.BACKTICK] = "`";
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_QUOTE_SINGLE] = {
    match: "'",
    push: LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.START_QUOTE_DOUBLE] = {
    match: '"',
    push: LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING
  };
  innerSqlBlockLexer[INNER_SQL_BLOCK_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const innerSingleQuoteLexer: moo.Rules = {};
  innerSingleQuoteLexer[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKSLASH] = "\\\\";
  innerSingleQuoteLexer[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_QUOTE] = "\\'";
  innerSingleQuoteLexer[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CLOSE_QUOTE] = {
    match: "'",
    pop: 1
  };
  innerSingleQuoteLexer[SQL_SINGLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const innerDoubleQuoteLexer: moo.Rules = {};
  innerDoubleQuoteLexer[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_BACKSLASH] = "\\\\";
  innerDoubleQuoteLexer[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.ESCAPED_QUOTE] = {
    match: '\\"'
  };
  innerDoubleQuoteLexer[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CLOSE_QUOTE] = {
    match: '"',
    pop: 1
  };
  innerDoubleQuoteLexer[SQL_DOUBLE_QUOTE_STRING_LEXER_TOKEN_NAMES.CAPTURE_EVERYTHING_ELSE] = {
    match: /[\s\S]+?/,
    lineBreaks: true
  };

  const lexerStates: { [x: string]: moo.Rules } = {};
  lexerStates[LEXER_STATE_NAMES.SQL] = sqlLexer;
  lexerStates[LEXER_STATE_NAMES.JS_BLOCK] = jsBlockLexer;
  lexerStates[LEXER_STATE_NAMES.JS_TEMPLATE_STRING] = jsTemplateStringLexer;
  lexerStates[LEXER_STATE_NAMES.INNER_SQL_BLOCK] = innerSqlBlockLexer;
  lexerStates[LEXER_STATE_NAMES.SQL_SINGLE_QUOTE_STRING] = innerSingleQuoteLexer;
  lexerStates[LEXER_STATE_NAMES.SQL_DOUBLE_QUOTE_STRING] = innerDoubleQuoteLexer;

  return lexerStates;
}
