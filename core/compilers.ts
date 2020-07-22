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
  publish("${utils.baseFilename(path)}").query(ctx => {
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
  operate("${utils.baseFilename(path)}").queries(ctx => {
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

function compileSqlx(rootNode: SyntaxTreeNode, path: string) {
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
            SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR
          ].includes(node.type)
      )
  );

  let incremental = "";
  let preOperations = [""];
  let postOperations = [""];
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

  return `
const parsedConfig = ${config || "{}"};
// sqlxConfig should conform to the ISqlxConfig interface.
const sqlxConfig = {
  name: "${utils.baseFilename(path)}",
  type: "operations",
  ...parsedConfig
};

const sqlStatementCount = ${sql.length};
const hasIncremental = ${!!incremental};
const hasPreOperations = ${preOperations.length > 1 || preOperations[0] !== ""};
const hasPostOperations = ${postOperations.length > 1 || postOperations[0] !== ""};
const hasInputs = ${Object.keys(inputs).length > 0};

const action = session.sqlxAction({
  sqlxConfig,
  sqlStatementCount,
  hasIncremental,
  hasPreOperations,
  hasPostOperations,
  hasInputs
});

switch (sqlxConfig.type) {
  case "view":
  case "table":
  case "incremental":
  case "inline": {
    action.query(ctx => {
      ${["self", "ref", "resolve", "name", "when", "incremental"]
        .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
        .join("\n")}
      ${js}
      if (hasIncremental) {
        action.where(\`${incremental}\`);
      }
      return \`${sql[0]}\`;
    });
    if (hasPreOperations) {
      action.preOps(ctx => {
        ${["self", "ref", "resolve", "name", "when", "incremental"]
          .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
          .join("\n")}
        ${js}
        return [${preOperations.map(preOpSql => `\`${preOpSql}\``)}];
      });
    }
    if (hasPostOperations) {
      action.postOps(ctx => {
        ${["self", "ref", "resolve", "name", "when", "incremental"]
          .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
          .join("\n")}
        ${js}
        return [${postOperations.map(postOpSql => `\`${postOpSql}\``)}];
      });
    }
    break;
  }
  case "assertion": {
    action.query(ctx => {
      ${["ref", "resolve"].map(name => `const ${name} = ctx.${name}.bind(ctx);`).join("\n")}
      ${js}
      return \`${sql[0]}\`;
    });
    break;
  }
  case "operations": {
    action.queries(ctx => {
      ${["self", "ref", "resolve", "name"]
        .map(name => `const ${name} = ctx.${name}.bind(ctx);`)
        .join("\n")}
      ${js}
      const operations = [${sql.map(sqlOp => `\`${sqlOp}\``)}];
      return operations;
    });
    break;
  }
  case "declaration": {
    break;
  }
  case "test": {
    ${inputs
      .map(
        ({ labelParts, value }) =>
          `
        action.input([${labelParts.map(labelPart => `"${labelPart}"`).join(", ")}], ctx => {
          ${js}
          return \`${value}\`;
        });
        `
      )
      .join("\n")}
    action.expect(ctx => {
      ${js}
      return \`${sql}\`;
    });
    break;
  }
  default: {
    session.compileError(new Error(\`Unrecognized action type: \${sqlxConfig.type}\`));
    break;
  }
}
`;
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
  nodes.map(escapeNode).forEach(node => {
    if (typeof node !== "string" && node.type === SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR) {
      results.push("");
      return;
    }
    results[results.length - 1] += typeof node === "string" ? node : node.concatenate();
  });
  return results;
}

function escapeNode(node: string | SyntaxTreeNode) {
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
      // Literal strings may contain backslashes or backticks which need to be escaped.
      return node
        .concatenate()
        .replace(/\\/g, "\\\\")
        .replace(/\`/g, "\\`");
  }
  return node;
}
