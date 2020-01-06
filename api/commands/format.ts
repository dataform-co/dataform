import { constructSyntaxTree, ISyntaxTreeNode } from "@dataform/sqlx/lexer";
import * as crypto from "crypto";
import * as fs from "fs";
import * as jsBeautify from "js-beautify";
import * as sqlFormatter from "sql-formatter";
import { promisify } from "util";

const JS_BEAUTIFY_OPTIONS: JsBeautifyOptions = {
  indent_size: 2,
  preserve_newlines: true,
  max_preserve_newlines: 2
};

const MAX_SQL_FORMAT_ATTEMPTS = 5;

const TEXT_LIFT_PATTERNS = [/r'.*'/, /r".*"/];

export async function formatFile(
  filename: string,
  options?: {
    overwriteFile?: boolean;
  }
) {
  const fileExtension = filename.split(".").slice(-1)[0];
  const format = (text: string) => {
    try {
      switch (fileExtension) {
        case "sqlx":
          return postProcessFormattedSqlx(formatSqlx(constructSyntaxTree(text)));
        case "js":
          return `${formatJavaScript(text).trim()}\n`;
        default:
          return text;
      }
    } catch (e) {
      throw new Error(`Unable to format "${filename}": ${e.message}`);
    }
  };
  const formattedText = format(await promisify(fs.readFile)(filename, "utf8"));
  if (formattedText !== format(formattedText)) {
    throw new Error("Formatter unable to determine final formatted form.");
  }
  if (options && options.overwriteFile) {
    await promisify(fs.writeFile)(filename, formattedText);
  }
  return formattedText;
}

function formatSqlx(node: ISyntaxTreeNode, indent: string = "") {
  const { codeBlocks, sqlxStatements } = separateCodeBlocksAndSqlxStatements(node.contents);

  // First, format the JS blocks (including the config block).
  const formattedJsCodeBlocks = codeBlocks
    .filter(codeBlock => codeBlock.contentType === "js")
    .map(jsCodeBlock => formatJavaScript(concatenateSyntaxTreeContents(jsCodeBlock)));

  // Second, format all the SQLX statements, replacing any placeholders with their formatted form.
  const formattedSqlxStatements = sqlxStatements.map(sqlxStatement => {
    const placeholders: {
      [placeholderId: string]: ISyntaxTreeNode | string;
    } = {};
    const unformattedPlaceholderSql = stripUnformattableText(sqlxStatement, placeholders).join("");
    const formattedPlaceholderSql = formatSql(unformattedPlaceholderSql);
    return formatEveryLine(
      replacePlaceholders(formattedPlaceholderSql, placeholders),
      line => `${indent}${line}`
    );
  });

  // Third, format all "inner" SQL blocks, e.g. "pre_operations { ... }".
  const formattedSqlCodeBlocks = codeBlocks
    .filter(codeBlock => codeBlock.contentType === "sql")
    .map((sqlCodeBlock): string => {
      // Strip out the declaration of this block, format the internals then add the declaration back.
      const firstPart = sqlCodeBlock.contents[0] as string;
      const upToFirstBrace = firstPart.slice(0, firstPart.indexOf("{") + 1);
      sqlCodeBlock.contents[0] = firstPart.slice(firstPart.indexOf("{") + 1);

      const lastPart = sqlCodeBlock.contents[sqlCodeBlock.contents.length - 1] as string;
      const lastBraceOnwards = lastPart.slice(lastPart.lastIndexOf("}"));
      sqlCodeBlock.contents[sqlCodeBlock.contents.length - 1] = lastPart.slice(
        0,
        lastPart.lastIndexOf("}")
      );

      return `${upToFirstBrace}
${formatSqlx(sqlCodeBlock, "  ")}
${lastBraceOnwards}`;
    });

  const finalText = `
${formattedJsCodeBlocks.join("\n\n")}

${formattedSqlxStatements.join(`\n\n${indent}---\n\n`)}

${formattedSqlCodeBlocks.join("\n\n")}
`;
  return `${indent}${finalText.trim()}`;
}

function separateCodeBlocksAndSqlxStatements(nodeContents: Array<string | ISyntaxTreeNode>) {
  const codeBlocks: ISyntaxTreeNode[] = [];
  const sqlxStatements: Array<Array<string | ISyntaxTreeNode>> = [[]];
  nodeContents.forEach(child => {
    if (
      typeof child === "string" ||
      child.contentType === "jsPlaceholder" ||
      child.contentType === "sqlComment"
    ) {
      sqlxStatements[sqlxStatements.length - 1].push(child);
      return;
    }
    if (child.contentType === "sqlStatementSeparator") {
      sqlxStatements.push([]);
      return;
    }
    codeBlocks.push(child);
  });
  return {
    codeBlocks,
    sqlxStatements
  };
}

function stripUnformattableText(
  sqlxStatementParts: Array<string | ISyntaxTreeNode>,
  placeholders: {
    [placeholderId: string]: ISyntaxTreeNode | string;
  }
) {
  return sqlxStatementParts.map(part => {
    if (typeof part !== "string") {
      const placeholderId = generatePlaceholderId();
      switch (part.contentType) {
        case "jsPlaceholder": {
          placeholders[placeholderId] = part;
          return placeholderId;
        }
        case "sqlComment": {
          // sql-formatter knows how to format comments (as long as they keep to a single line);
          // give it a hint.
          const commentPlaceholderId = concatenateSyntaxTreeContents(part).startsWith("--")
            ? `--${placeholderId}`
            : `/*${placeholderId}*/`;
          placeholders[commentPlaceholderId] = part;
          return commentPlaceholderId;
        }
        default:
          throw new Error(
            `Misplaced syntax node content type inside SQLX query: ${part.contentType}`
          );
      }
    }
    for (const pattern of TEXT_LIFT_PATTERNS) {
      while (part.match(pattern)) {
        const placeholderId = generatePlaceholderId();
        placeholders[placeholderId] = part.match(pattern)[0];
        part = part.replace(pattern, placeholderId);
      }
    }
    return part;
  });
}

function generatePlaceholderId() {
  return crypto.randomBytes(16).toString("hex");
}

function replacePlaceholders(
  formattedSql: string,
  placeholders: {
    [placeholderId: string]: ISyntaxTreeNode | string;
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

function formatSql(text: string) {
  let formatted = sqlFormatter.format(text) as string;
  // Unfortunately sql-formatter does not always produce final formatted output (even on plain SQL) in a single pass.
  for (let attempts = 0; attempts < MAX_SQL_FORMAT_ATTEMPTS; attempts++) {
    const newFormatted = sqlFormatter.format(formatted) as string;
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
  placeholderSyntaxNode: ISyntaxTreeNode,
  sqlx: string
) {
  const wholeLine = getWholeLineContainingPlaceholderId(placeholderId, sqlx);
  const indent = " ".repeat(wholeLine.length - wholeLine.trimLeft().length);
  const formattedPlaceholder = formatSqlQueryPlaceholder(placeholderSyntaxNode, indent);
  // Replace the placeholder entirely if (a) it fits on one line and (b) it isn't a comment.
  // Otherwise, push the replacement onto its own line.
  if (placeholderSyntaxNode.contentType !== "sqlComment" && !formattedPlaceholder.includes("\n")) {
    return sqlx.replace(placeholderId, formattedPlaceholder.trim());
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

function formatSqlQueryPlaceholder(node: ISyntaxTreeNode, jsIndent: string): string {
  switch (node.contentType) {
    case "jsPlaceholder":
      return formatJavaScriptPlaceholder(node, jsIndent);
    case "sqlComment":
      return formatEveryLine(
        concatenateSyntaxTreeContents(node),
        line => `${jsIndent}${line.trimLeft()}`
      );
    default:
      throw new Error(`Unrecognized syntax node content type: ${node.contentType}`);
  }
}

function formatJavaScriptPlaceholder(node: ISyntaxTreeNode, jsIndent: string) {
  const formattedJs = formatJavaScript(concatenateSyntaxTreeContents(node));
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
  return text.match(new RegExp(".*" + regexpEscapedPlaceholderId + ".*"))[0];
}

function concatenateSyntaxTreeContents(node: ISyntaxTreeNode): string {
  return node.contents
    .map(content => {
      if (typeof content === "string") {
        return content;
      }
      return concatenateSyntaxTreeContents(content);
    })
    .join("");
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
