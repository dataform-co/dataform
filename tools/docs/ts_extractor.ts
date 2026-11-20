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

export function extractCoreExports(coreIndexPath: string, packageIndexPath?: string): IExportDoc[] {
  const sourceFile = parseSourceFile(coreIndexPath);
  const importedInterfaceAliases = collectImportedInterfaceAliases(sourceFile);
  const allCoreExportNames = new Set<string>();
  const publicExports: IExportDoc[] = [];

  sourceFile.forEachChild((node) => {
    if (ts.isExportDeclaration(node)) {
      throw new Error(
        `${coreIndexPath}: unexpected re-export declaration; declare public exports directly with JSDoc so types and descriptions can be extracted.`,
      );
    }
    if (!hasExportModifier(node)) {
      return;
    }
    const isInternal = hasInternalJsDocTag(node);

    if (ts.isVariableStatement(node)) {
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      for (const decl of node.declarationList.declarations) {
        if (!ts.isIdentifier(decl.name)) {
          continue;
        }
        allCoreExportNames.add(decl.name.text);
        if (isInternal) {
          continue;
        }
        if (!descriptionMarkdown) {
          throw new Error(
            `${coreIndexPath}: exported variable "${decl.name.text}" is missing a JSDoc summary.`,
          );
        }
        publicExports.push({
          name: decl.name.text,
          typeLabel: decl.type ? collapseWhitespace(decl.type.getText(sourceFile)) : "unknown",
          descriptionMarkdown,
        });
      }
    } else if (ts.isFunctionDeclaration(node) && node.name) {
      allCoreExportNames.add(node.name.text);
      if (isInternal) {
        return;
      }
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${coreIndexPath}: exported function "${node.name.text}" is missing a JSDoc summary.`,
        );
      }
      const returnType = node.type ? node.type.getText(sourceFile) : "void";
      publicExports.push({
        name: node.name.text,
        typeLabel: `${formatParameterList(node.parameters, sourceFile)} => ${collapseWhitespace(returnType)}`,
        descriptionMarkdown,
      });
    } else if (ts.isInterfaceDeclaration(node)) {
      allCoreExportNames.add(node.name.text);
      if (isInternal) {
        return;
      }
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${coreIndexPath}: exported interface "${node.name.text}" is missing a JSDoc summary.`,
        );
      }
      publicExports.push({
        name: node.name.text,
        typeLabel: "interface",
        descriptionMarkdown,
      });
    } else if (ts.isTypeAliasDeclaration(node)) {
      allCoreExportNames.add(node.name.text);
      if (isInternal) {
        return;
      }
      const descriptionMarkdown = extractFirstJsDocParagraph(node, sourceFile);
      if (!descriptionMarkdown) {
        throw new Error(
          `${coreIndexPath}: exported type "${node.name.text}" is missing a JSDoc summary.`,
        );
      }
      const isInterfaceAlias =
        ts.isTypeReferenceNode(node.type) &&
        ts.isIdentifier(node.type.typeName) &&
        importedInterfaceAliases.has(node.type.typeName.text);
      publicExports.push({
        name: node.name.text,
        typeLabel: isInterfaceAlias ? "interface" : "type",
        descriptionMarkdown,
      });
    }
  });

  if (packageIndexPath) {
    verifyPackageEntrypointParity(coreIndexPath, allCoreExportNames, packageIndexPath);
  }

  return publicExports;
}

export function extractNamedReExports(entrypointPath: string): Set<string> {
  const sourceFile = parseSourceFile(entrypointPath);
  const names = new Set<string>();
  sourceFile.forEachChild((node) => {
    if (ts.isExportDeclaration(node) && node.exportClause && ts.isNamedExports(node.exportClause)) {
      for (const element of node.exportClause.elements) {
        names.add(element.name.text);
      }
    }
  });
  return names;
}

function verifyPackageEntrypointParity(
  coreIndexPath: string,
  coreExportNames: Set<string>,
  packageIndexPath: string,
): void {
  const packageExportNames = extractNamedReExports(packageIndexPath);
  const missingInPackage = [...coreExportNames].filter((name) => !packageExportNames.has(name));
  const missingInCore = [...packageExportNames].filter((name) => !coreExportNames.has(name));
  if (missingInPackage.length > 0 || missingInCore.length > 0) {
    const details: string[] = [];
    if (missingInPackage.length > 0) {
      details.push(`missing in ${packageIndexPath}: ${missingInPackage.join(", ")}`);
    }
    if (missingInCore.length > 0) {
      details.push(`missing in ${coreIndexPath}: ${missingInCore.join(", ")}`);
    }
    throw new Error(
      `Export mismatch between ${coreIndexPath} and ${packageIndexPath} (${details.join("; ")}).`,
    );
  }
}

function collectImportedInterfaceAliases(sourceFile: ts.SourceFile): Set<string> {
  const aliases = new Set<string>();
  sourceFile.forEachChild((node) => {
    if (
      ts.isImportDeclaration(node) &&
      node.importClause?.namedBindings &&
      ts.isNamedImports(node.importClause.namedBindings)
    ) {
      for (const element of node.importClause.namedBindings.elements) {
        const originalName = (element.propertyName || element.name).text;
        if (/^I[A-Z]/.test(originalName)) {
          aliases.add(element.name.text);
        }
      }
    }
  });
  return aliases;
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
  const jsDocs = ts.getJSDocCommentsAndTags(node).filter(ts.isJSDoc);
  if (jsDocs.length === 0) {
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
