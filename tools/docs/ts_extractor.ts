import * as fs from "fs";
import * as ts from "typescript";

import { SQLX_CONTEXT_FUNCTIONS } from "df/core/compilers";
import { SESSION_GLOBAL_METHODS } from "df/core/session";

export interface IFunctionDoc {
  name: string;
  signatureMarkdown: string;
  descriptionMarkdown: string;
}

export interface IExportDoc {
  name: string;
  typeLabel: string;
  descriptionMarkdown: string;
}

export function extractSqlxContextFunctions(contextablesPath: string): IFunctionDoc[] {
  const sourceFile = parseSourceFile(contextablesPath);
  const interfaces = new Map<string, ts.InterfaceDeclaration>();

  sourceFile.forEachChild((node) => {
    if (ts.isInterfaceDeclaration(node)) {
      interfaces.set(node.name.text, node);
    }
  });

  const actionContext = interfaces.get("IActionContext");
  const tableContext = interfaces.get("ITableContext");
  if (!actionContext || !tableContext) {
    throw new Error(
      `${contextablesPath}: expected both IActionContext and ITableContext interfaces.`,
    );
  }

  const docs: IFunctionDoc[] = [
    ...extractInterfaceMethodDocs(actionContext, sourceFile),
    ...extractInterfaceMethodDocs(tableContext, sourceFile),
  ];

  const documentedNames = new Set(docs.map((doc) => doc.name));
  for (const requiredName of SQLX_CONTEXT_FUNCTIONS) {
    if (!documentedNames.has(requiredName)) {
      throw new Error(
        `${contextablesPath}: SQLX context function "${requiredName}" from SQLX_CONTEXT_FUNCTIONS ` +
          "is missing from IActionContext / ITableContext.",
      );
    }
  }
  return docs;
}

export function extractSessionGlobals(sessionPath: string): IFunctionDoc[] {
  const sourceFile = parseSourceFile(sessionPath);
  let sessionClass: ts.ClassDeclaration | undefined;

  sourceFile.forEachChild((node) => {
    if (ts.isClassDeclaration(node) && node.name?.text === "Session") {
      sessionClass = node;
    }
  });

  if (!sessionClass) {
    throw new Error(`${sessionPath}: expected exported class Session.`);
  }

  const methodsByName = new Map<string, ts.MethodDeclaration>();
  let projectConfigProperty: ts.PropertyDeclaration | undefined;

  for (const member of sessionClass.members) {
    if (ts.isMethodDeclaration(member) && member.name && ts.isIdentifier(member.name)) {
      methodsByName.set(member.name.text, member);
    } else if (
      ts.isPropertyDeclaration(member) &&
      member.name &&
      ts.isIdentifier(member.name) &&
      member.name.text === "projectConfig"
    ) {
      projectConfigProperty = member;
    }
  }

  const docs: IFunctionDoc[] = SESSION_GLOBAL_METHODS.map((methodName) => {
    const method = methodsByName.get(methodName);
    if (!method) {
      throw new Error(
        `${sessionPath}: SESSION_GLOBAL_METHODS entry "${methodName}" was not found on class Session.`,
      );
    }
    const descriptionMarkdown = extractFirstJsDocParagraph(method, sourceFile);
    if (!descriptionMarkdown) {
      throw new Error(
        `${sessionPath}: Session.${methodName}() is missing a JSDoc summary comment.`,
      );
    }
    const customSignature = extractJsDocTagText(method, "signature", sourceFile);
    return {
      name: methodName,
      signatureMarkdown:
        customSignature || `\`${formatCallSignature(methodName, method.parameters, sourceFile)}\``,
      descriptionMarkdown,
    };
  });

  if (!projectConfigProperty) {
    throw new Error(`${sessionPath}: expected Session.projectConfig property.`);
  }
  const projectConfigType = projectConfigProperty.type
    ? projectConfigProperty.type.getText(sourceFile)
    : "unknown";
  const projectConfigDoc = extractFirstJsDocParagraph(projectConfigProperty, sourceFile);
  if (!projectConfigDoc) {
    throw new Error(`${sessionPath}: Session.projectConfig is missing a JSDoc summary comment.`);
  }
  docs.push({
    name: "dataform.projectConfig",
    signatureMarkdown: `\`${projectConfigType}\``,
    descriptionMarkdown: projectConfigDoc,
  });

  return docs;
}

export function extractCoreExports(coreIndexPath: string): IExportDoc[] {
  const sourceFile = parseSourceFile(coreIndexPath);
  const exports: IExportDoc[] = [];

  sourceFile.forEachChild((node) => {
    if (!hasExportModifier(node) || hasInternalJsDocTag(node)) {
      return;
    }

    if (ts.isVariableStatement(node)) {
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      for (const decl of node.declarationList.declarations) {
        if (!ts.isIdentifier(decl.name)) {
          continue;
        }
        if (!descriptionMarkdown) {
          throw new Error(
            `${coreIndexPath}: exported variable "${decl.name.text}" is missing a JSDoc summary.`,
          );
        }
        exports.push({
          name: decl.name.text,
          typeLabel: decl.type ? collapseWhitespace(decl.type.getText(sourceFile)) : "unknown",
          descriptionMarkdown,
        });
      }
    } else if (ts.isFunctionDeclaration(node) && node.name) {
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${coreIndexPath}: exported function "${node.name.text}" is missing a JSDoc summary.`,
        );
      }
      const returnType = node.type ? node.type.getText(sourceFile) : "void";
      exports.push({
        name: node.name.text,
        typeLabel: `${formatParameterList(node.parameters, sourceFile)} => ${collapseWhitespace(returnType)}`,
        descriptionMarkdown,
      });
    } else if (ts.isTypeAliasDeclaration(node) || ts.isInterfaceDeclaration(node)) {
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${coreIndexPath}: exported type "${node.name.text}" is missing a JSDoc summary.`,
        );
      }
      exports.push({
        name: node.name.text,
        typeLabel: "interface",
        descriptionMarkdown,
      });
    }
  });

  return exports;
}

function parseSourceFile(filePath: string): ts.SourceFile {
  const content = fs.readFileSync(filePath, "utf8");
  return ts.createSourceFile(filePath, content, ts.ScriptTarget.Latest, true);
}

function extractInterfaceMethodDocs(
  iface: ts.InterfaceDeclaration,
  sourceFile: ts.SourceFile,
): IFunctionDoc[] {
  const docs: IFunctionDoc[] = [];
  for (const member of iface.members) {
    if (ts.isPropertySignature(member) && member.name && ts.isIdentifier(member.name)) {
      const name = member.name.text;
      const descriptionMarkdown = extractFirstJsDocParagraph(member, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${sourceFile.fileName}: ${iface.name.text}.${name} is missing a JSDoc summary comment.`,
        );
      }
      const customSignature = extractJsDocTagText(member, "signature", sourceFile);
      const rawSignature =
        member.type && ts.isFunctionTypeNode(member.type)
          ? formatCallSignature(name, member.type.parameters, sourceFile)
          : name;
      docs.push({
        name,
        signatureMarkdown: customSignature || `\`${rawSignature}\``,
        descriptionMarkdown,
      });
    } else if (ts.isMethodSignature(member) && member.name && ts.isIdentifier(member.name)) {
      const name = member.name.text;
      const descriptionMarkdown = extractFirstJsDocParagraph(member, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${sourceFile.fileName}: ${iface.name.text}.${name}() is missing a JSDoc summary comment.`,
        );
      }
      const customSignature = extractJsDocTagText(member, "signature", sourceFile);
      docs.push({
        name,
        signatureMarkdown:
          customSignature || `\`${formatCallSignature(name, member.parameters, sourceFile)}\``,
        descriptionMarkdown,
      });
    }
  }
  return docs;
}

function formatCallSignature(
  name: string,
  parameters: ts.NodeArray<ts.ParameterDeclaration>,
  sourceFile: ts.SourceFile,
): string {
  return `${name}${formatParameterList(parameters, sourceFile)}`;
}

function formatParameterList(
  parameters: ts.NodeArray<ts.ParameterDeclaration>,
  sourceFile: ts.SourceFile,
): string {
  const formatted = parameters.map((param) => {
    const prefix = param.dotDotDotToken ? "..." : "";
    const paramName = param.name.getText(sourceFile);
    const optional = param.questionToken || param.initializer ? "?" : "";
    return `${prefix}${paramName}${optional}`;
  });
  return `(${formatted.join(", ")})`;
}

function extractFirstJsDocParagraph(node: ts.Node, sourceFile: ts.SourceFile): string {
  const jsDocs = (node as { jsDoc?: ts.JSDoc[] }).jsDoc;
  if (!jsDocs || jsDocs.length === 0) {
    return "";
  }
  const latestDoc = jsDocs[jsDocs.length - 1];
  const rawComment = getJsDocCommentText(latestDoc.comment, sourceFile);
  if (!rawComment) {
    return "";
  }
  const firstParagraph = rawComment.trim().split(/\r?\n\s*\r?\n/)[0];
  return collapseWhitespace(firstParagraph);
}

function extractJsDocTagText(node: ts.Node, tagName: string, sourceFile: ts.SourceFile): string {
  const tag = ts.getJSDocTags(node).find((candidate) => candidate.tagName.text === tagName);
  if (!tag) {
    return "";
  }
  return collapseWhitespace(getJsDocCommentText(tag.comment, sourceFile));
}

function getJsDocCommentText(
  comment: string | ts.NodeArray<ts.JSDocComment> | undefined,
  sourceFile: ts.SourceFile,
): string {
  if (!comment) {
    return "";
  }
  if (typeof comment === "string") {
    return comment;
  }
  return comment.map((part) => part.getText(sourceFile)).join("");
}

function hasExportModifier(node: ts.Node): boolean {
  if (!ts.canHaveModifiers(node)) {
    return false;
  }
  const modifiers = ts.getModifiers(node);
  return Boolean(modifiers?.some((mod) => mod.kind === ts.SyntaxKind.ExportKeyword));
}

function hasInternalJsDocTag(node: ts.Node): boolean {
  return ts.getJSDocTags(node).some((tag) => tag.tagName.text === "internal");
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}
