import { constructSyntaxTree, ISyntaxTreeNode } from "@dataform/sqlx/lexer";
import * as crypto from "crypto";
import * as fs from "fs";
import * as jsBeautify from "js-beautify";
import * as sqlFormatter from "sql-formatter";
import { promisify } from "util";

const JS_BEAUTIFY_OPTIONS: JsBeautifyOptions = {
  indent_size: 2,
  preserve_newlines: false
};

export async function formatFile(
  filename: string,
  options?: {
    overwriteFile: boolean;
  }
) {
  const fileText = await promisify(fs.readFile)(filename, "utf8");
  const fileExtension = filename.split(".").slice(-1)[0];
  const formattedText = (() => {
    try {
      switch (fileExtension) {
        case "sqlx":
          return formatSqlx(constructSyntaxTree(fileText));
        case "js":
          return formatJavaScript(fileText);
        default:
          return fileText;
      }
    } catch (e) {
      throw new Error(`Unable to format "${filename}": ${e.message}`);
    }
  })();
  if (options && options.overwriteFile) {
    await promisify(fs.writeFile)(filename, fileText);
  }
  return formattedText;
}

function formatSqlx(node: ISyntaxTreeNode) {
  const placeholders: {
    [placeholderId: string]: ISyntaxTreeNode;
  } = {};
  const unformattedPlaceholderSql = node.contents
    .map(child => {
      if (typeof child !== "string") {
        const placeholderId = crypto.randomBytes(16).toString("hex");
        placeholders[placeholderId] = child;
        return placeholderId;
      }
      return child;
    })
    .join("");
  return postProcessFormattedSqlx(
    Object.keys(placeholders).reduce(
      (partiallyFormattedSql, placeholderId) =>
        formatPlaceholderInSqlx(placeholderId, placeholders[placeholderId], partiallyFormattedSql),
      sqlFormatter.format(unformattedPlaceholderSql) as string
    )
  );
}

function formatJavaScript(text: string) {
  return jsBeautify.js(text, JS_BEAUTIFY_OPTIONS);
}

function formatPlaceholderInSqlx(
  placeholderId: string,
  placeholderSyntaxNode: ISyntaxTreeNode,
  sqlx: string
) {
  const wholeLineContainingPlaceholderId = getWholeLineContainingPlaceholderId(placeholderId, sqlx);
  // Figure out if the placeholder is on its own line; if so,
  // replace the whole line, passing any necessary indentation.
  const indentSize =
    wholeLineContainingPlaceholderId.trim() === placeholderId
      ? wholeLineContainingPlaceholderId.length - wholeLineContainingPlaceholderId.trimLeft().length
      : 0;
  const formattedChild = formatChildSyntaxTreeNode(placeholderSyntaxNode, indentSize);
  const textToReplace =
    wholeLineContainingPlaceholderId.trim() === placeholderId
      ? wholeLineContainingPlaceholderId
      : placeholderId;
  return sqlx.replace(textToReplace, formattedChild);
}

function formatChildSyntaxTreeNode(node: ISyntaxTreeNode, jsIndent: number): string {
  if (node.startTokenType.endsWith("_startJsPlaceholder")) {
    return formatJavaScriptPlaceholder(node, jsIndent);
  }
  if (node.startTokenType.endsWith("_startJs") || node.startTokenType.endsWith("_startConfig")) {
    return `\n\n${formatJavaScript(concatenateSyntaxTreeContents(node))}\n\n`;
  }
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

function formatJavaScriptPlaceholder(node: ISyntaxTreeNode, jsIndent: number = 0) {
  const formattedJs = formatJavaScript(concatenateSyntaxTreeContents(node));
  const afterFirstBrace = formattedJs.slice(formattedJs.indexOf("{") + 1);
  return `\${${afterFirstBrace}`
    .split("\n")
    .map(line => `${" ".repeat(jsIndent)}${line}`)
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
