import { load as loadYaml, YAMLException } from "js-yaml";

import * as Path from "df/core/path";
import { SyntaxTreeNode, SyntaxTreeNodeType } from "df/sqlx/lexer";

const CONTEXT_FUNCTIONS = [
  "self",
  "ref",
  "resolve",
  "name",
  "when",
  "incremental",
  "schema",
  "database"
]
    .map(name => `const ${name} = ctx.${name} ? ctx.${name}.bind(ctx) : undefined;`)
    .join("\n");

const CONTEXT_CONSTANTS = [
  "EXPECT"
]
    .map(name => `const ${name} = ctx.${name} ? ctx.${name} : undefined;`)
    .join("\n");

const JIT_CONTEXT_FUNCTIONS = [
  "self",
  "ref",
  "resolve",
  "name",
  "when",
  "incremental",
  "schema",
  "database"
].map(name => `const ${name} = jctx.${name} ? jctx.${name}.bind(jctx) : undefined;`)
  .join("\n    ");

const JIT_CONTEXT_CONSTANTS = [
  "adapter",
  "data",
]
  .map(name => `const ${name} = jctx.${name} ? jctx.${name} : undefined;`)
  .join("\n    ");

export const INVALID_YAML_ERROR_STRING = "is not a valid YAML file";

export function compile(code: string, path: string): string {
  if (Path.fileExtension(path) === "sqlx") {
    return compileSqlx(SyntaxTreeNode.create(code), path);
  }
  if (Path.fileExtension(path) === "yaml" || Path.fileExtension(path) === "yml") {
    try {
      const yamlAsJson = loadYaml(code);
      return `exports.asJson = ${JSON.stringify(yamlAsJson)}`;
    } catch (e) {
      if (e instanceof YAMLException) {
        throw new Error(`${path} ${INVALID_YAML_ERROR_STRING}: ${e}`);
      }
      throw e;
    }
  }
  if (Path.fileExtension(path) === "ipynb") {
    let codeAsJson = {};
    try {
      codeAsJson = JSON.parse(code);
    } catch (e) {
      throw new Error(`Error parsing ${path} as JSON: ${e}`);
    }
    const notebookAsJson = JSON.stringify(codeAsJson);
    return `exports.asJson = ${notebookAsJson}`;
  }
  if (Path.fileExtension(path) === "sql") {
    const escapedCode = code
      .replace(/\\/g, "\\\\")
      .replace(/`/g, "\\`")
      .replace(/\${/g, "\\${");
    return `exports.query = \`${escapedCode}\`;`;
  }
  return code;
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

function compileSqlx(rootNode: SyntaxTreeNode, path: string): string {
  const sqlxParts = extractSqlxParts(
    rootNode
  );
  const { config } = sqlxParts;
  try {
    // Part of the compilation, to be evalled in main compilation via require anyway.
    // tslint:disable-next-line: tsr-detect-eval-with-expression
    const sqlxConfig: { [key: string]: unknown } | undefined = new Function(`return ${config || "{}"}`)();
    if (!!sqlxConfig && sqlxConfig.hasOwnProperty("compilation_mode") && sqlxConfig.compilation_mode === "jit") {
      const actionType = sqlxConfig.hasOwnProperty("type") ? sqlxConfig.type : "operations";
      if (typeof actionType !== "string") {
        throw new Error(`Invalid action type: ${actionType} at ${path}.`);
      }
      return compileSqlxJit(sqlxParts, path, actionType);
    }
  } catch (_) {
    // Let AoT compiler handle JS parsing errors
  }

  return compileSqlxAot(sqlxParts, path);
}

function makeSqlxJitReturnBlock(parts: SqlxParts, actionType: string): string {
  const { sql, preOperations, postOperations } = parts;
  switch (actionType) {
    case "operations":
      return `return {
        queries: [
          ${sql.map(sqlOp => `\`${sqlOp}\``)}
        ],
      };`;
    case "incremental":
    case "table":
    case "view":
      if (sql.length > 1) {
        throw new Error("Table and views must have at most 1 SQL statement.");
      }
      return `return {
        query: (
          \`${sql[0]}\`
        ),
        postOps: (
          ${postOperations.length > 0 ? ('[' + postOperations.map(postOpSql => `\`${postOpSql}\``) + ']') : 'undefined'}
        ),
        preOps: (
          ${preOperations.length > 0 ? ('[' + preOperations.map(preOpSql => `\`${preOpSql}\``) + ']') : 'undefined'}
        ),
      };`;
    default:
      throw new Error(`Invalid action type: ${actionType}`);
  }
}

function compileSqlxJit(parts: SqlxParts, path: string, actionType: string): string {
  const { config, js } = parts;
  const returnBlock = makeSqlxJitReturnBlock(parts, actionType);

  return `dataform.sqlxJitAction({
  sqlxConfig: {
    name: "${Path.escapedBasename(path)}",
    type: "operations",
    ...${config || "{}"}
  },
  jitCode: "` + `async (jctx) => {
    ${JIT_CONTEXT_FUNCTIONS}
    ${JIT_CONTEXT_CONSTANTS}
    ${js}
    ${returnBlock}
    }`
    .replace(/\\/, "\\\\")
    .replace(/\r?\n/g, "\\n")
    .replace(/\"/g, "\\\"") + `"
  });
  `;
}

function compileSqlxAot(parts: SqlxParts, path: string): string {
  const { config, js, sql, incremental, preOperations, postOperations, inputs } = parts;

  return `dataform.sqlxAction({
  sqlxConfig: {
    name: "${Path.escapedBasename(path)}",
    type: "operations",
    ...${config || "{}"}
  },
  sqlStatementCount: ${sql.length},
  sqlContextable: (ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
    ${js}
    return [${sql.map(sqlOp => `\`${sqlOp}\``)}];
  },
  incrementalWhereContextable: ${
    !!incremental
      ? `(ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
    ${js}
    return \`${incremental}\`
  }`
      : "undefined"
  },
  preOperationsContextable: ${
    preOperations.length > 0
      ? `(ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
    ${js}
    return [${preOperations.map(preOpSql => `\`${preOpSql}\``)}];
  }`
      : "undefined"
  },
  postOperationsContextable: ${
    postOperations.length > 0
      ? `(ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
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

type SqlxParts = ReturnType<typeof extractSqlxParts>;

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
  ]
]);

function escapeNode(node: string | SyntaxTreeNode) {
  if (typeof node === "string") {
    return SQL_STATEMENT_ESCAPERS.get(SyntaxTreeNodeType.SQL_LITERAL_STRING)(node);
  }
  return node.concatenate(SQL_STATEMENT_ESCAPERS);
}
