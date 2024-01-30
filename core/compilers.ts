import { AssertionContext } from "df/core/assertion";
import { OperationContext } from "df/core/operation";
import { TableContext } from "df/core/table";
import * as utils from "df/core/utils";
import { SyntaxTreeNode, SyntaxTreeNodeType } from "df/sqlx/lexer";

export function compile(code: string, path: string) {
  if (path.endsWith(".sqlx")) {
    return compileSqlx(SyntaxTreeNode.create(code), path);
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

// For older versions of @dataform/core, these functions may not actually exist so leave them as undefined.
function safelyBindCtxFunction(name: string) {
  return `ctx.${name} ? ctx.${name}.bind(ctx) : undefined`;
}

function compileTableSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(TableContext.prototype).map(
    name => `const ${name} = ${safelyBindCtxFunction(name)};`
  );

  return `
  publish("${utils.getEscapedFileName(path)}").query(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`;
  })`;
}

function compileOperationSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(OperationContext.prototype).map(
    name => `const ${name} = ${safelyBindCtxFunction(name)};`
  );

  return `
  operate("${utils.getEscapedFileName(path)}").queries(ctx => {
    ${functionsBindings.join("\n")}
    ${js}
    return \`${sql}\`.split("\\n---\\n");
  })`;
}

function compileAssertionSql(code: string, path: string) {
  const { sql, js } = extractJsBlocks(code);
  const functionsBindings = getFunctionPropertyNames(AssertionContext.prototype).map(
    name => `const ${name} = ${safelyBindCtxFunction(name)};`
  );

  return `
  assert("${utils.getEscapedFileName(path)}").query(ctx => {
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

export function compileStandaloneSqlxQuery(code: string) {
  const { config, js, sql, incremental, preOperations, postOperations, inputs } = extractSqlxParts(
    SyntaxTreeNode.create(code)
  );
  if (config) {
    throw new Error(`Standalone SQLX queries may not define 'config' code blocks.`);
  }
  if (incremental) {
    throw new Error(`Standalone SQLX queries may not define 'incremental_where' code blocks.`);
  }
  if (preOperations.length > 0) {
    throw new Error(`Standalone SQLX queries may not define 'pre_operations' code blocks.`);
  }
  if (postOperations.length > 0) {
    throw new Error(`Standalone SQLX queries may not define 'post_operations' code blocks.`);
  }
  if (inputs.length > 0) {
    throw new Error(`Standalone SQLX queries may not define 'input' code blocks.`);
  }
  return `
    const ref = dataform.resolve.bind(dataform);
    const resolve = dataform.resolve.bind(dataform);
    ${js}
    return \`${sql}\`;
  `;
}

function compileSqlx(rootNode: SyntaxTreeNode, path: string) {
  const { config, js, sql, incremental, preOperations, postOperations, inputs } = extractSqlxParts(
    rootNode
  );

  const contextFunctions = ["self", "ref", "resolve", "name", "when", "incremental", "schema", "database"]
    .map(name => `const ${name} = ctx.${name} ? ctx.${name}.bind(ctx) : undefined;`)
    .join("\n");

  return `dataform.sqlxAction({
  sqlxConfig: {
    name: "${utils.getEscapedFileName(path)}",
    type: "operations",
    ...${config || "{}"}
  },
  sqlStatementCount: ${sql.length},
  sqlContextable: (ctx) => {
    ${contextFunctions}
    ${js}
    return [${sql.map(sqlOp => `\`${sqlOp}\``)}];
  },
  incrementalWhereContextable: ${
    !!incremental
      ? `(ctx) => {
    ${contextFunctions}
    ${js}
    return \`${incremental}\`
  }`
      : "undefined"
  },
  preOperationsContextable: ${
    preOperations.length > 0
      ? `(ctx) => {
    ${contextFunctions}
    ${js}
    return [${preOperations.map(preOpSql => `\`${preOpSql}\``)}];
  }`
      : "undefined"
  },
  postOperationsContextable: ${
    postOperations.length > 0
      ? `(ctx) => {
    ${contextFunctions}
    ${js}
    return [${postOperations.map(postOpSql => `\`${postOpSql}\``)}];
  }`
      : "undefined"
  },
  inputContextables: [
    ${inputs
      .map(
        ({ labelParts, value }) =>
          `{
            refName: [${labelParts.map(labelPart => `"${labelPart}"`).join(", ")}],
            contextable: (ctx) => {
              ${js}
              return \`${value}\`;
            }
          }`
      )
      .join(",")}
  ]
});
`;
}

function extractSqlxParts(rootNode: SyntaxTreeNode) {
  let config = "";
  let js = "";
  rootNode
    .children()
    .filter(SyntaxTreeNode.isSyntaxTreeNode)
    .filter(node => node.type === SyntaxTreeNodeType.JAVASCRIPT)
    .forEach(node => {
      const concatenated = node.concatenate();
      if (concatenated.startsWith("config")) {
        config = concatenated.slice("config ".length);
      } else {
        js += concatenated.slice("js {".length, "}".length * -1);
      }
    });

  const sql = createEscapedStatements(
    rootNode
      .children()
      .filter(
        node =>
          typeof node === "string" ||
          [
            SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER,
            SyntaxTreeNodeType.SQL_COMMENT,
            SyntaxTreeNodeType.SQL_LITERAL_STRING,
            SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING,
            SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR
          ].includes(node.type)
      )
  );

  let incremental = "";
  let preOperations: string[] = [];
  let postOperations: string[] = [];
  const inputs: Array<{
    labelParts: string[];
    value: string;
  }> = [];
  rootNode
    .children()
    .filter(SyntaxTreeNode.isSyntaxTreeNode)
    .filter(node => node.type === SyntaxTreeNodeType.SQL)
    .forEach(node => {
      const firstChild = node.children()[0] as string;
      const lastChild = node.children().slice(-1)[0] as string;

      const sqlCodeBlockWithoutOuterBraces =
        node.children().length === 1
          ? new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
              firstChild.slice(firstChild.indexOf("{") + 1, firstChild.lastIndexOf("}"))
            ])
          : new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
              firstChild.slice(firstChild.indexOf("{") + 1),
              ...node.children().slice(1, -1),
              lastChild.slice(0, lastChild.lastIndexOf("}"))
            ]);
      const statements = createEscapedStatements(sqlCodeBlockWithoutOuterBraces.children());

      if (firstChild.startsWith("incremental_where")) {
        if (statements.length > 1) {
          throw new Error(
            "'incremental_where' code blocks may only contain a single SQL statement."
          );
        }
        incremental = statements[0];
      } else if (firstChild.startsWith("pre_operations")) {
        preOperations = statements;
      } else if (firstChild.startsWith("post_operations")) {
        postOperations = statements;
      } else if (firstChild.startsWith("input")) {
        if (statements.length > 1) {
          throw new Error("'input' code blocks may only contain a single SQL statement.");
        }
        const labelParts = firstChild
          .slice(firstChild.indexOf('"'), firstChild.lastIndexOf('"') + 1)
          .split(",")
          .map(label => label.trim().slice(1, -1));
        inputs.push({
          labelParts,
          value: statements[0]
        });
      }
    });

  return {
    config,
    js,
    sql,
    incremental,
    preOperations,
    postOperations,
    inputs
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

function createEscapedStatements(nodes: Array<string | SyntaxTreeNode>) {
  const results = [""];
  nodes.forEach(node => {
    if (typeof node !== "string" && node.type === SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR) {
      results.push("");
      return;
    }
    results[results.length - 1] += escapeNode(node);
  });
  return results;
}

const SQL_STATEMENT_ESCAPERS = new Map([
  [
    SyntaxTreeNodeType.SQL_COMMENT,
    (str: string) => str.replace(/`/g, "\\`").replace(/\${/g, "\\${")
  ],
  [
    SyntaxTreeNodeType.SQL_LITERAL_STRING,
    (str: string) => str.replace(/\\/g, "\\\\").replace(/\`/g, "\\`")
  ],
  [
    SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING,
    (str: string) => str.replace(/\\/g, "\\\\").replace(/\`/g, "\\`")
  ],
]);

function escapeNode(node: string | SyntaxTreeNode) {
  if (typeof node === "string") {
    return SQL_STATEMENT_ESCAPERS.get(SyntaxTreeNodeType.SQL_LITERAL_STRING)(node);
  }
  return node.concatenate(SQL_STATEMENT_ESCAPERS);
}
