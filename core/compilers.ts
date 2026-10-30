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
  "database",
]
  .map((name) => `const ${name} = ctx.${name} ? ctx.${name}.bind(ctx) : undefined;`)
  .join("\n");

const CONTEXT_CONSTANTS = ["EXPECT"]
  .map((name) => `const ${name} = ctx.${name} ? ctx.${name} : undefined;`)
  .join("\n");

const JIT_CONTEXT_FUNCTIONS = [
  "self",
  "ref",
  "resolve",
  "name",
  "when",
  "incremental",
  "schema",
  "database",
]
  .map((name) => `const ${name} = jctx.${name} ? jctx.${name}.bind(jctx) : undefined;`)
  .join("\n    ");

const JIT_CONTEXT_CONSTANTS = ["adapter", "data", "executionData"]
  .map((name) => `const ${name} = jctx.${name} ? jctx.${name} : undefined;`)
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
    const escapedCode = code.replace(/\\/g, "\\\\").replace(/`/g, "\\`").replace(/\${/g, "\\${");
    return `exports.query = \`${escapedCode}\`;`;
  }
  if (Path.fileExtension(path) === "md") {
    const contents = code
      .replace(/\r\n/g, "\n")
      .replace(/\\/g, "\\\\")
      .replace(/`/g, "\\`")
      .replace(/\${/g, "\\${");
    return `exports.contents = \`${contents}\`;`;
  }
  return code;
}

function compileSqlx(rootNode: SyntaxTreeNode, path: string): string {
  const sqlxParts = extractSqlxParts(rootNode);
  const { config } = sqlxParts;
  const configWithoutComments = config.replace(/\/\*[\s\S]*?\*\/|\/\/.*/g, "");
  const compilationModeMatch =
    /(?:^|[{,\s])(?:compilation_mode|["']compilation_mode["'])\s*:\s*(?:(["'])(.*?)\1|([^\s,}]+))/.exec(
      configWithoutComments,
    );
  if (compilationModeMatch) {
    if (compilationModeMatch[1] !== undefined) {
      const mode = compilationModeMatch[2];
      if (mode === "jit") {
        const staticActionType = extractStaticActionType(configWithoutComments, path);
        return compileSqlxJit(sqlxParts, path, staticActionType);
      }
      if (mode !== "aot") {
        throw new Error(`Invalid compilation_mode "${mode}" in ${path}; expected "jit" or "aot".`);
      }
    } else {
      const rawToken = compilationModeMatch[3];
      throw new Error(
        `Invalid compilation_mode "${rawToken}" in ${path}; expected "jit" or "aot".`,
      );
    }
  }

  return compileSqlxAot(sqlxParts, path);
}

function extractStaticActionType(configWithoutComments: string, path: string): string | undefined {
  const typeMatch = /(?:^|[{,\s])(?:type|["']type["'])\s*:\s*(?:(["'])(.*?)\1|([^,}]+))/.exec(
    configWithoutComments,
  );
  if (!typeMatch) {
    return "operations";
  }
  if (typeMatch[1] !== undefined) {
    return typeMatch[2];
  }
  const rawTypeExpr = typeMatch[3].trim();
  if (/^(?:true|false|null|undefined|-?\d+(?:\.\d+)?)$/.test(rawTypeExpr)) {
    throw new Error(`Invalid action type: ${rawTypeExpr} at ${path}.`);
  }
  return undefined;
}

export interface ISqlxBlockValidationOptions {
  sqlStatementCount: number;
  hasIncrementalWhere: boolean;
  hasInputs: boolean;
  hasPreOperations: boolean;
  hasPostOperations: boolean;
  isJit: boolean;
}

export function validateSqlxActionBlocks(
  actionType: string,
  options: ISqlxBlockValidationOptions,
  onError: (message: string) => void,
) {
  if (options.sqlStatementCount > 1 && actionType !== "operations") {
    onError(
      "Actions may only contain more than one SQL statement if they are of type 'operations'.",
    );
  }
  if (options.hasIncrementalWhere) {
    if (options.isJit) {
      onError("incremental_where is not supported with JiT compilation.");
    } else if (actionType !== "incremental") {
      onError("Actions may only include incremental_where if they are of type 'incremental'.");
    }
  }
  if (options.hasInputs) {
    if (options.isJit) {
      onError("input blocks are not supported with JiT compilation.");
    } else if (actionType !== "test") {
      onError("Actions may only include input blocks if they are of type 'test'.");
    }
  }
  const definesDataset = ["incremental", "table", "view"].includes(actionType);
  if (options.hasPreOperations && !definesDataset) {
    onError("Actions may only include pre_operations if they create a dataset.");
  }
  if (options.hasPostOperations && !definesDataset) {
    onError("Actions may only include post_operations if they create a dataset.");
  }
}

function makeSqlxJitReturnBlock(parts: SqlxParts, actionType: string): string {
  const { sql, preOperations, postOperations } = parts;
  switch (actionType) {
    case "operations":
      return `return {
        queries: [
          ${sql.map((sqlOp) => `\`${sqlOp}\``).join(", ")}
        ],
      };`;
    case "assertion":
      return `return {
        query: (
          \`${sql[0]}\`
        ),
      };`;
    case "incremental":
    case "table":
    case "view":
      return `return {
        query: (
          \`${sql[0]}\`
        ),
        postOps: (
          ${postOperations.length > 0 ? "[" + postOperations.map((postOpSql) => `\`${postOpSql}\``).join(", ") + "]" : "undefined"}
        ),
        preOps: (
          ${preOperations.length > 0 ? "[" + preOperations.map((preOpSql) => `\`${preOpSql}\``).join(", ") + "]" : "undefined"}
        ),
      };`;
    default:
      throw new Error(`Unsupported by JiT compilation mode action type: ${actionType}`);
  }
}

function buildJitCodeString(js: string, returnBlock: string): string {
  const jitCode = `async (jctx) => {
    ${JIT_CONTEXT_FUNCTIONS}
    ${JIT_CONTEXT_CONSTANTS}
    ${js}
    ${returnBlock}
    }`;
  // Syntax-check generated jitCode at compile time without executing it.
  // eslint-disable-next-line no-new-func
  new Function("jctx", `const mainAsync = ${jitCode};`);
  return jitCode;
}

function compileSqlxJit(
  parts: SqlxParts,
  path: string,
  staticActionType: string | undefined,
): string {
  const { config, js, sql, incremental, preOperations, postOperations, inputs } = parts;
  const blockFlags = {
    sqlStatementCount: sql.length,
    hasIncrementalWhere: Boolean(incremental),
    hasInputs: inputs.length > 0,
    hasPreOperations: preOperations.length > 0,
    hasPostOperations: postOperations.length > 0,
  };
  if (staticActionType !== undefined) {
    validateSqlxActionBlocks(staticActionType, { ...blockFlags, isJit: true }, (msg) => {
      throw new Error(msg);
    });
    const returnBlock = makeSqlxJitReturnBlock(parts, staticActionType);
    const jitCode = buildJitCodeString(js, returnBlock);
    return `dataform.sqlxJitAction({
  sqlxConfig: {
    name: ${JSON.stringify(Path.basename(path))},
    type: "operations",
    ...${config || "{}"}
  },
  jitCode: ${JSON.stringify(jitCode)},
  sqlStatementCount: ${blockFlags.sqlStatementCount},
  hasIncrementalWhere: ${blockFlags.hasIncrementalWhere},
  hasInputs: ${blockFlags.hasInputs},
  hasPreOperations: ${blockFlags.hasPreOperations},
  hasPostOperations: ${blockFlags.hasPostOperations}
  });
  `;
  }

  // When `type` is a dynamic expression (e.g. referencing `dataform.*` or `includes`),
  // defer actionType-dependent validation and return-block selection to Session.sqlxJitAction.
  const jitCodeByType = {
    operations: buildJitCodeString(js, makeSqlxJitReturnBlock(parts, "operations")),
    assertion: buildJitCodeString(js, makeSqlxJitReturnBlock(parts, "assertion")),
    dataset: buildJitCodeString(js, makeSqlxJitReturnBlock(parts, "table")),
  };
  return `dataform.sqlxJitAction({
  sqlxConfig: {
    name: ${JSON.stringify(Path.basename(path))},
    type: "operations",
    ...${config || "{}"}
  },
  jitCode: ${JSON.stringify(jitCodeByType.dataset)},
  jitCodeByType: ${JSON.stringify(jitCodeByType)},
  sqlStatementCount: ${blockFlags.sqlStatementCount},
  hasIncrementalWhere: ${blockFlags.hasIncrementalWhere},
  hasInputs: ${blockFlags.hasInputs},
  hasPreOperations: ${blockFlags.hasPreOperations},
  hasPostOperations: ${blockFlags.hasPostOperations}
  });
  `;
}

function compileSqlxAot(parts: SqlxParts, path: string): string {
  const { config, js, sql, incremental, preOperations, postOperations, inputs } = parts;

  return `dataform.sqlxAction({
  sqlxConfig: {
    name: ${JSON.stringify(Path.basename(path))},
    type: "operations",
    ...${config || "{}"}
  },
  sqlStatementCount: ${sql.length},
  sqlContextable: (ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
    ${js}
    return [${sql.map((sqlOp) => `\`${sqlOp}\``)}];
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
    return [${preOperations.map((preOpSql) => `\`${preOpSql}\``)}];
  }`
      : "undefined"
  },
  postOperationsContextable: ${
    postOperations.length > 0
      ? `(ctx) => {
    ${CONTEXT_FUNCTIONS}
    ${CONTEXT_CONSTANTS}
    ${js}
    return [${postOperations.map((postOpSql) => `\`${postOpSql}\``)}];
  }`
      : "undefined"
  },
  inputContextables: [
    ${inputs
      .map(
        ({ labelParts, value }) =>
          `{
            refName: [${labelParts.map((labelPart) => `"${labelPart}"`).join(", ")}],
            contextable: (ctx) => {
              ${js}
              return \`${value}\`;
            }
          }`,
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
    .filter((node) => node.type === SyntaxTreeNodeType.JAVASCRIPT)
    .forEach((node) => {
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
        (node) =>
          typeof node === "string" ||
          [
            SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER,
            SyntaxTreeNodeType.SQL_COMMENT,
            SyntaxTreeNodeType.SQL_LITERAL_STRING,
            SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING,
            SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR,
          ].includes(node.type),
      ),
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
    .filter((node) => node.type === SyntaxTreeNodeType.SQL)
    .forEach((node) => {
      const firstChild = node.children()[0] as string;
      const lastChild = node.children().slice(-1)[0] as string;

      const sqlCodeBlockWithoutOuterBraces =
        node.children().length === 1
          ? new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
              firstChild.slice(firstChild.indexOf("{") + 1, firstChild.lastIndexOf("}")),
            ])
          : new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
              firstChild.slice(firstChild.indexOf("{") + 1),
              ...node.children().slice(1, -1),
              lastChild.slice(0, lastChild.lastIndexOf("}")),
            ]);
      const statements = createEscapedStatements(sqlCodeBlockWithoutOuterBraces.children());

      if (firstChild.startsWith("incremental_where")) {
        if (statements.length > 1) {
          throw new Error(
            "'incremental_where' code blocks may only contain a single SQL statement.",
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
          .map((label) => label.trim().slice(1, -1));
        inputs.push({
          labelParts,
          value: statements[0],
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
    inputs,
  };
}

function createEscapedStatements(nodes: Array<string | SyntaxTreeNode>) {
  const results = [""];
  nodes.forEach((node) => {
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
    (str: string) => str.replace(/\\/g, "\\\\").replace(/`/g, "\\`").replace(/\${/g, "\\${"),
  ],
  [
    SyntaxTreeNodeType.SQL_LITERAL_STRING,
    (str: string) => str.replace(/\\/g, "\\\\").replace(/\`/g, "\\`"),
  ],
  [
    SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING,
    (str: string) => str.replace(/\\/g, "\\\\").replace(/\`/g, "\\`"),
  ],
]);

function escapeNode(node: string | SyntaxTreeNode) {
  if (typeof node === "string") {
    return SQL_STATEMENT_ESCAPERS.get(SyntaxTreeNodeType.SQL_LITERAL_STRING)(node);
  }
  return node.concatenate(SQL_STATEMENT_ESCAPERS);
}
