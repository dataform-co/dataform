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

const TEXT_LIFT_PATTERNS = [/r'(\\['\\]|[^\n'\\])*'/, /"(\\["\\]|[^\n"\\])*"/];

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

function formatSqlx(node: ISyntaxTreeNode) {
  return getIndividualSqlxStatements(node.contents)
    .map(individualStatement => {
      const placeholders: {
        [placeholderId: string]: ISyntaxTreeNode | string;
      } = {};
      const unformattedPlaceholderSql = stripUnformattableText(
        individualStatement,
        placeholders
      ).join("");
      const formattedPlaceholderSql = sqlFormatter.format(unformattedPlaceholderSql) as string;
      return replacePlaceholders(formattedPlaceholderSql, placeholders);
    })
    .join("\n\n---\n\n");
}

function getIndividualSqlxStatements(nodeContents: Array<string | ISyntaxTreeNode>) {
  const sqlxStatements: Array<Array<string | ISyntaxTreeNode>> = [[]];
  nodeContents.forEach(child => {
    if (typeof child !== "string" && child.contentType === "sqlStatementSeparator") {
      sqlxStatements.push([]);
    } else {
      sqlxStatements[sqlxStatements.length - 1].push(child);
    }
  });
  return sqlxStatements;
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
      placeholders[placeholderId] = part;
      return placeholderId;
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

function formatPlaceholderInSqlx(
  placeholderId: string,
  placeholderSyntaxNode: ISyntaxTreeNode,
  sqlx: string
) {
  const wholeLine = getWholeLineContainingPlaceholderId(placeholderId, sqlx);
  const indent = " ".repeat(wholeLine.length - wholeLine.trimLeft().length);
  const formattedPlaceholder = formatChildSyntaxTreeNode(placeholderSyntaxNode, indent);
  // If the formatted placeholder doesn't include linebreaks, just replace it entirely.
  if (!formattedPlaceholder.includes("\n")) {
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

function formatChildSyntaxTreeNode(node: ISyntaxTreeNode, jsIndent: string): string {
  switch (node.contentType) {
    case "jsPlaceholder":
      return formatJavaScriptPlaceholder(node, jsIndent);
    case "js":
      return `\n\n${formatJavaScript(concatenateSyntaxTreeContents(node))}\n\n`;
    case "sql": {
      // This node must be an "inner" SQL block, e.g. "pre_operations { ... }",
      // so strip out the declaration of that block, format the internals,
      // then add the declaration back.
      const firstPart = node.contents[0] as string;
      const upToFirstBrace = firstPart.slice(0, firstPart.indexOf("{") + 1);
      node.contents[0] = firstPart.slice(firstPart.indexOf("{") + 1);

      const lastPart = node.contents[node.contents.length - 1] as string;
      const lastBraceOnwards = lastPart.slice(lastPart.lastIndexOf("}"));
      node.contents[node.contents.length - 1] = lastPart.slice(0, lastPart.lastIndexOf("}"));

      return `\n
${upToFirstBrace}
${formatSqlx(node)
  .split("\n")
  .map(line => `  ${line}`)
  .join("\n")
  .trimRight()}
${lastBraceOnwards}
\n`;
    }
    default:
      // This shouldn't happen, but if it does, handle it by simply returning unformatted text.
      return concatenateSyntaxTreeContents(node);
  }
}

function formatJavaScriptPlaceholder(node: ISyntaxTreeNode, jsIndent: string = "") {
  const formattedJs = formatJavaScript(concatenateSyntaxTreeContents(node));
  const textInsideBraces = formattedJs.slice(
    formattedJs.indexOf("{") + 1,
    formattedJs.lastIndexOf("}")
  );
  // If the formatted JS is only a single line, trim all whitespace so that it stays a single line.
  const finalJs = textInsideBraces.trim().includes("\n")
    ? `\${${textInsideBraces}}`
    : `\${${textInsideBraces.trim()}}`;
  return finalJs
    .split("\n")
    .map(line => `${jsIndent}${line}`)
    .join("\n");
}

function getWholeLineContainingPlaceholderId(placeholderId: string, text: string) {
  return text.match(new RegExp(".*" + placeholderId + ".*"))[0];
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
