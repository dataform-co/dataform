import * as fs from "fs";
import * as jsBeautify from "js-beautify";
import * as sqlFormatter from "sql-formatter";
import { promisify } from "util";

import { ErrorWithCause } from "df/common/errors/errors";
import { WarehouseType } from "df/core/adapters";
import { SyntaxTreeNode, SyntaxTreeNodeType } from "df/sqlx/lexer";
import { v4 as uuidv4 } from "uuid";

const JS_BEAUTIFY_OPTIONS = {
  indent_size: 2,
  preserve_newlines: true,
  max_preserve_newlines: 2
};

const MAX_SQL_FORMAT_ATTEMPTS = 5;

const WAREHOUSE_LANGUAGE_MAP: Record<WarehouseType, sqlFormatter.SqlLanguage> = {
  [WarehouseType.BIGQUERY]: "bigquery",
  [WarehouseType.PRESTO]: "trino",
  [WarehouseType.POSTGRES]: "postgresql",
  [WarehouseType.REDSHIFT]: "redshift",
  [WarehouseType.SNOWFLAKE]: "snowflake",
  [WarehouseType.SQLDATAWAREHOUSE]: "transactsql"
};

const DEFAULT_WAREHOUSE_FOR_FORMATTING: WarehouseType = WarehouseType.BIGQUERY;

export function format(
  text: string,
  fileExtension: string,
  warehouse: WarehouseType = DEFAULT_WAREHOUSE_FOR_FORMATTING
) {
  try {
    switch (fileExtension) {
      case "sqlx":
        return postProcessFormattedSqlx(formatSqlx(SyntaxTreeNode.create(text), "", warehouse));
      case "js":
        return `${formatJavaScript(text).trim()}\n`;
      default:
        return text;
    }
  } catch (e) {
    throw new ErrorWithCause(`Unable to format "${text?.substring(0, 20)}...".`, e);
  }
}

export async function formatFile(
  filename: string,
  options?: {
    overwriteFile?: boolean;
    warehouse?: WarehouseType;
  }
) {
  const fileExtension = filename.split(".").slice(-1)[0];
  const originalFileContent = await promisify(fs.readFile)(filename, "utf8");

  const formattedText = format(originalFileContent, fileExtension, options?.warehouse);
  if (formattedText !== format(formattedText, fileExtension, options?.warehouse)) {
    throw new Error("Formatter unable to determine final formatted form.");
  }

  const noWhiteSpaceFormatted = formattedText.replace(/\s/g, "");
  const noWhiteSpaceOriginal = originalFileContent.replace(/\s/g, "");
  if (noWhiteSpaceFormatted.length !== noWhiteSpaceOriginal.length) {
    const isLonger = noWhiteSpaceFormatted.length > noWhiteSpaceOriginal.length;
    throw new Error(`Formatter ${isLonger ? "added" : "removed"} non-whitespace characters`);
  }

  if (options && options.overwriteFile) {
    await promisify(fs.writeFile)(filename, formattedText);
  }
  return formattedText;
}

function formatSqlx(node: SyntaxTreeNode, indent: string = "", warehouse: WarehouseType) {
  const { sqlxStatements, javascriptBlocks, innerSqlBlocks } = separateSqlxIntoParts(
    node.children()
  );
  const sqlLanguage = WAREHOUSE_LANGUAGE_MAP[warehouse];

  // First, format the JS blocks (including the config block).
  const formattedJsCodeBlocks = javascriptBlocks.map(jsCodeBlock =>
    formatJavaScript(jsCodeBlock.concatenate())
  );

  // Second, format all the SQLX statements, replacing any placeholders with their formatted form.
  const formattedSqlxStatements = sqlxStatements.map(sqlxStatement => {
    const placeholders: {
      [placeholderId: string]: SyntaxTreeNode | string;
    } = {};
    const unformattedPlaceholderSql = stripUnformattableText(sqlxStatement, placeholders).join("");
    const formattedPlaceholderSql = formatSql(unformattedPlaceholderSql, sqlLanguage);
    return formatEveryLine(
      replacePlaceholders(formattedPlaceholderSql, placeholders),
      line => `${indent}${line}`
    );
  });

  // Third, format all "inner" SQL blocks, e.g. "pre_operations { ... }".
  const formattedSqlCodeBlocks = innerSqlBlocks.map((sqlCodeBlock): string => {
    // Strip out the declaration of this block, format the internals then add the declaration back.
    const firstPart = sqlCodeBlock.children()[0] as string;
    const upToFirstBrace = firstPart.slice(0, firstPart.indexOf("{") + 1);

    const lastPart = sqlCodeBlock.children()[sqlCodeBlock.children().length - 1] as string;
    const lastBraceOnwards = lastPart.slice(lastPart.lastIndexOf("}"));

    const sqlCodeBlockWithoutOuterBraces =
      sqlCodeBlock.children().length === 1
        ? new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
            firstPart.slice(firstPart.indexOf("{") + 1, firstPart.lastIndexOf("}"))
          ])
        : new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
            firstPart.slice(firstPart.indexOf("{") + 1),
            ...sqlCodeBlock.children().slice(1, -1),
            lastPart.slice(0, lastPart.lastIndexOf("}"))
          ]);

    return `${upToFirstBrace}
${formatSqlx(sqlCodeBlockWithoutOuterBraces, "  ", warehouse)}
${lastBraceOnwards}`;
  });

  const finalText = `
${formattedJsCodeBlocks.join("\n\n")}

${formattedSqlxStatements.join(`\n\n${indent}---\n\n`)}

${formattedSqlCodeBlocks.join("\n\n")}
`;
  return `${indent}${finalText.trim()}`;
}

function separateSqlxIntoParts(nodeContents: Array<string | SyntaxTreeNode>) {
  const sqlxStatements: Array<Array<string | SyntaxTreeNode>> = [[]];
  const javascriptBlocks: SyntaxTreeNode[] = [];
  const innerSqlBlocks: SyntaxTreeNode[] = [];
  nodeContents.forEach(child => {
    if (typeof child !== "string") {
      switch (child.type) {
        case SyntaxTreeNodeType.JAVASCRIPT:
          javascriptBlocks.push(child);
          return;
        case SyntaxTreeNodeType.SQL:
          innerSqlBlocks.push(child);
          return;
        case SyntaxTreeNodeType.SQL_STATEMENT_SEPARATOR:
          sqlxStatements.push([]);
          return;
      }
    }
    sqlxStatements[sqlxStatements.length - 1].push(child);
  });
  return {
    sqlxStatements,
    javascriptBlocks,
    innerSqlBlocks
  };
}

function stripUnformattableText(
  sqlxStatementParts: Array<string | SyntaxTreeNode>,
  placeholders: {
    [placeholderId: string]: SyntaxTreeNode | string;
  }
) {
  return sqlxStatementParts.map(part => {
    if (typeof part !== "string") {
      const placeholderId = generatePlaceholderId();
      switch (part.type) {
        case SyntaxTreeNodeType.SQL_LITERAL_STRING:
        case SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING:
        case SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER: {
          placeholders[placeholderId] = part;
          return placeholderId;
        }
        case SyntaxTreeNodeType.SQL_COMMENT: {
          // sql-formatter knows how to format comments (as long as they keep to a single line);
          // give it a hint.
          const commentPlaceholderId = part.concatenate().startsWith("--")
            ? `--${placeholderId}`
            : `/*${placeholderId}*/`;
          placeholders[commentPlaceholderId] = part;
          return commentPlaceholderId;
        }
        default:
          throw new Error(`Misplaced SyntaxTreeNodeType inside SQLX: ${part.type}`);
      }
    }
    return part;
  });
}

function generatePlaceholderId() {
  // Add a leading character to ensure that the placeholder doesn't start with a number.
  // Identifiers beginning with a number cause errors when formatting.
  return '_' + uuidv4()
    .replace(/-/g, "")
    .substring(0, 16);
}

function replacePlaceholders(
  formattedSql: string,
  placeholders: {
    [placeholderId: string]: SyntaxTreeNode | string;
  }
) {
  return Object.keys(placeholders).reduce((partiallyFormattedSql, placeholderId) => {
    const placeholderValue = placeholders[placeholderId];
    if (typeof placeholderValue === "string") {
      return partiallyFormattedSql.replace(placeholderId, placeholderValue);
    }
    return formatPlaceholderInSqlx(placeholderId, placeholderValue, partiallyFormattedSql);
  }, formattedSql);
}

function formatJavaScript(text: string) {
  return jsBeautify.js(text, JS_BEAUTIFY_OPTIONS);
}

function formatSql(text: string, language: sqlFormatter.SqlLanguage) {
  let formatted = sqlFormatter.format(text, { language }) as string;
  // Unfortunately sql-formatter does not always produce final formatted output (even on plain SQL) in a single pass.
  for (let attempts = 0; attempts < MAX_SQL_FORMAT_ATTEMPTS; attempts++) {
    const newFormatted = sqlFormatter.format(formatted, { language }) as string;
    if (newFormatted === formatted) {
      return newFormatted;
    }
    formatted = newFormatted;
  }
  throw new Error(
    `SQL formatter was unable to determine final formatted form within ${MAX_SQL_FORMAT_ATTEMPTS} attempts. Original text: ${text}`
  );
}

function formatPlaceholderInSqlx(
  placeholderId: string,
  placeholderSyntaxNode: SyntaxTreeNode,
  sqlx: string
) {
  const wholeLine = getWholeLineContainingPlaceholderId(placeholderId, sqlx);
  const indent = " ".repeat(wholeLine.length - wholeLine.trimLeft().length);
  const formattedPlaceholder = formatSqlQueryPlaceholder(placeholderSyntaxNode, indent);

  // Replace the placeholder entirely if (a) it fits on one line and (b) it isn't a comment.
  // Otherwise, push the replacement onto its own line.
  if (
    placeholderSyntaxNode.type !== SyntaxTreeNodeType.SQL_COMMENT &&
    !formattedPlaceholder.includes("\n")
  ) {
    return sqlx.replace(placeholderId, () => formattedPlaceholder.trim());
  }

  // Keep internal line breaks in multiline string.
  if (placeholderSyntaxNode.type === SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING) {
    return sqlx.replace(placeholderId, () => formattedPlaceholder.trim());
  }

  // Push multi-line placeholders to their own lines, if they're not already on one.
  const [textBeforePlaceholder, textAfterPlaceholder] = wholeLine.split(placeholderId);
  const newLines: string[] = [];
  if (textBeforePlaceholder.trim().length > 0) {
    newLines.push(`${indent}${textBeforePlaceholder.trim()}`);
  }
  newLines.push(formattedPlaceholder);
  if (textAfterPlaceholder.trim().length > 0) {
    newLines.push(`${indent}${textAfterPlaceholder.trim()}`);
  }
  return sqlx.replace(wholeLine, newLines.join("\n"));
}

function formatSqlQueryPlaceholder(node: SyntaxTreeNode, jsIndent: string): string {
  switch (node.type) {
    case SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER:
      return formatJavaScriptPlaceholder(node, jsIndent);
    case SyntaxTreeNodeType.SQL_LITERAL_STRING:
    case SyntaxTreeNodeType.SQL_COMMENT:
      return formatEveryLine(node.concatenate(), line => `${jsIndent}${line.trimLeft()}`);
    case SyntaxTreeNodeType.SQL_LITERAL_MULTILINE_STRING:
      return `${jsIndent}${node.concatenate().trimLeft()}`;
    default:
      throw new Error(`Unrecognized SyntaxTreeNodeType: ${node.type}`);
  }
}

function formatJavaScriptPlaceholder(node: SyntaxTreeNode, jsIndent: string) {
  const formattedJs = formatJavaScript(node.concatenate());
  const textInsideBraces = formattedJs.slice(
    formattedJs.indexOf("{") + 1,
    formattedJs.lastIndexOf("}")
  );
  // If the formatted JS is only a single line, trim all whitespace so that it stays a single line.
  const finalJs = textInsideBraces.trim().includes("\n")
    ? `\${${textInsideBraces}}`
    : `\${${textInsideBraces.trim()}}`;
  return formatEveryLine(finalJs, line => `${jsIndent}${line}`);
}

function formatEveryLine(text: string, mapFn: (line: string) => string) {
  return text
    .split("\n")
    .map(mapFn)
    .join("\n");
}

function getWholeLineContainingPlaceholderId(placeholderId: string, text: string) {
  const regexpEscapedPlaceholderId = placeholderId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  // This RegExp is safe because we only use a 'placeholderId' that this file has generated.
  // tslint:disable-next-line: tsr-detect-non-literal-regexp
  return text.match(new RegExp(".*" + regexpEscapedPlaceholderId + ".*"))[0];
}

function postProcessFormattedSqlx(formattedSql: string) {
  let previousLineHadContent = false;
  formattedSql = formattedSql.split("\n").reduce((accumulatedSql, currentLine) => {
    const lineHasContent = currentLine.trim().length > 0;
    if (lineHasContent) {
      previousLineHadContent = true;
      return `${accumulatedSql}\n${currentLine.trimRight()}`;
    }
    if (previousLineHadContent) {
      previousLineHadContent = false;
      return `${accumulatedSql}\n`;
    }
    return accumulatedSql;
  }, "");
  return `${formattedSql.trim()}\n`;
}
