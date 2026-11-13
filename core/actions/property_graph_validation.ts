import { Structs } from "df/common/protos/structs";

const SEMANTIC_ONTOLOGY_GOOGLE_VERSION = "0.2.0.dev0/google";
const CLOSED_AI_CONTEXT_KEYS = ["instructions", "synonyms", "examples"] as const;

export function normalizeAndValidateAiContext(container: any, where: string): void {
  if (typeof container.aiContext === "string") {
    container.aiContext = { instructions: container.aiContext };
    return;
  }
  if (
    !container.aiContext ||
    typeof container.aiContext !== "object" ||
    Array.isArray(container.aiContext)
  ) {
    return;
  }
  if (!("custom" in container.aiContext) || container.aiContext.custom === undefined) {
    return;
  }

  const rawCustom = container.aiContext.custom;
  if (!rawCustom || typeof rawCustom !== "object" || Array.isArray(rawCustom)) {
    throw new Error(`${where}: 'ai_context.custom' must be a key-value mapping object.`);
  }
  if (Object.prototype.hasOwnProperty.call(rawCustom, "__proto__")) {
    throw new Error(`${where}: 'ai_context.custom' member '__proto__' is not supported.`);
  }
  for (const closedKey of CLOSED_AI_CONTEXT_KEYS) {
    if (Object.prototype.hasOwnProperty.call(rawCustom, closedKey)) {
      throw new Error(
        `${where}: 'ai_context.custom' cannot hold '${closedKey}'; ` +
          `write '${closedKey}' directly under 'ai_context'.`,
      );
    }
  }
  validateStructCompatibleValue(rawCustom, `${where}: 'ai_context.custom'`);
  container.aiContext = {
    ...container.aiContext,
    custom: Structs.fromObject(rawCustom),
  };
}

function validateStructCompatibleValue(value: unknown, where: string): void {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      validateStructCompatibleValue(item, where);
    }
    return;
  }
  if (typeof value === "object") {
    for (const entryValue of Object.values(value as Record<string, unknown>)) {
      validateStructCompatibleValue(entryValue, where);
    }
    return;
  }
  throw new Error(
    `${where} contains an unsupported value of type '${typeof value}'; ` +
      `expected strings, finite numbers, booleans, null, arrays, or plain objects.`,
  );
}

export function normalizeAndValidateCustomExtensions(container: any, where: string): void {
  if (!("customExtensions" in container) || container.customExtensions === undefined) {
    return;
  }
  if (!Array.isArray(container.customExtensions)) {
    throw new Error(`${where}: 'custom_extensions' must be a list.`);
  }

  container.customExtensions = container.customExtensions.map((ext: any, idx: number) => {
    if (!ext || typeof ext !== "object" || Array.isArray(ext)) {
      throw new Error(
        `${where}: 'custom_extensions[${idx}]' must be an object with 'vendor_name' and 'data'.`,
      );
    }
    if (typeof ext.vendorName !== "string" || ext.vendorName.trim().length === 0) {
      throw new Error(
        `${where}: every 'custom_extensions' entry must declare a non-empty 'vendor_name'.`,
      );
    }
    const vendorTrimmed = ext.vendorName.trim();
    if (vendorTrimmed.toUpperCase() === "GOOGLE") {
      throw new Error(
        `${where}: 'GOOGLE' custom_extensions blocks are not accepted in ` +
          `'${SEMANTIC_ONTOLOGY_GOOGLE_VERSION}' documents; declare Google extensions using native keys.`,
      );
    }
    if (typeof ext.data === "string") {
      return ext;
    }
    if (ext.data !== null && typeof ext.data === "object" && !Array.isArray(ext.data)) {
      return {
        ...ext,
        data: JSON.stringify(ext.data),
      };
    }
    throw new Error(
      `${where}: 'custom_extensions' entry for vendor '${ext.vendorName}' must declare 'data' ` +
        `as a JSON string or mapping object.`,
    );
  });
}

export function validateMetricExpression(expression: string, where: string): void {
  for (const rawArg of extractCountCallArguments(expression)) {
    const classification = classifyInvalidCountArgument(rawArg);
    if (classification === "star") {
      throw new Error(
        `${where}: 'COUNT(*)' is not supported in BigQuery property graph measure expressions; ` +
          `count an explicit property instead (for example, 'COUNT(id)').`,
      );
    }
    if (classification === "literal") {
      const displayArg = stripSqlCommentsPreservingLiterals(rawArg).trim();
      throw new Error(
        `${where}: 'COUNT(${displayArg})' is not supported in BigQuery property graph measure ` +
          `expressions because a measure must aggregate at least one graph property; ` +
          `count an explicit property instead (for example, 'COUNT(id)').`,
      );
    }
  }
}

function extractCountCallArguments(sql: string): string[] {
  const args: string[] = [];
  let i = 0;
  while (i < sql.length) {
    const skipped = skipCommentOrQuotedToken(sql, i);
    if (skipped > i) {
      i = skipped;
      continue;
    }

    if (
      sql.slice(i, i + 5).toUpperCase() === "COUNT" &&
      (i === 0 || !/[A-Za-z0-9_$]/.test(sql[i - 1])) &&
      !/[A-Za-z0-9_$]/.test(sql[i + 5] || "")
    ) {
      const openParen = skipWhitespaceAndComments(sql, i + 5);
      if (openParen < sql.length && sql[openParen] === "(") {
        const closeParen = findMatchingCloseParen(sql, openParen);
        if (closeParen !== -1) {
          args.push(sql.slice(openParen + 1, closeParen));
          i = openParen + 1;
          continue;
        }
      }
    }
    i++;
  }
  return args;
}

function classifyInvalidCountArgument(rawArg: string): "star" | "literal" | null {
  let cleaned = stripSqlCommentsPreservingLiterals(rawArg).trim();
  if (!cleaned) {
    return null;
  }

  cleaned = unwrapOuterParens(cleaned);

  const modifierMatch = /^(?:DISTINCT|ALL)(?=\s|\()/i.exec(cleaned);
  if (modifierMatch) {
    cleaned = unwrapOuterParens(cleaned.slice(modifierMatch[0].length).trim());
  }

  if (cleaned === "*") {
    return "star";
  }
  if (/^(?:TRUE|FALSE|NULL)$/i.test(cleaned)) {
    return "literal";
  }
  if (
    /^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(cleaned) ||
    /^[+-]?0x[0-9a-f]+$/i.test(cleaned)
  ) {
    return "literal";
  }

  const stringStart = getQuotedStringStart(cleaned, 0);
  if (stringStart !== -1 && consumeQuotedString(cleaned, stringStart) === cleaned.length) {
    return "literal";
  }

  return null;
}

function unwrapOuterParens(expr: string): string {
  let current = expr.trim();
  while (current.startsWith("(") && findMatchingCloseParen(current, 0) === current.length - 1) {
    current = current.slice(1, -1).trim();
  }
  return current;
}

function stripSqlCommentsPreservingLiterals(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const commentEnd = consumeSqlComment(sql, i);
    if (commentEnd > i) {
      out += " ";
      i = commentEnd;
      continue;
    }
    const quotedEnd = consumeQuotedOrBackticked(sql, i);
    if (quotedEnd > i) {
      out += sql.slice(i, quotedEnd);
      i = quotedEnd;
      continue;
    }
    out += sql[i];
    i++;
  }
  return out;
}

function skipWhitespaceAndComments(sql: string, start: number): number {
  let i = start;
  while (i < sql.length) {
    if (/\s/.test(sql[i])) {
      i++;
      continue;
    }
    const commentEnd = consumeSqlComment(sql, i);
    if (commentEnd > i) {
      i = commentEnd;
      continue;
    }
    break;
  }
  return i;
}

function findMatchingCloseParen(sql: string, openParenIdx: number): number {
  let depth = 0;
  let i = openParenIdx;
  while (i < sql.length) {
    const skipped = skipCommentOrQuotedToken(sql, i);
    if (skipped > i) {
      i = skipped;
      continue;
    }
    if (sql[i] === "(") {
      depth++;
    } else if (sql[i] === ")") {
      depth--;
      if (depth === 0) {
        return i;
      }
    }
    i++;
  }
  return -1;
}

function skipCommentOrQuotedToken(sql: string, index: number): number {
  const commentEnd = consumeSqlComment(sql, index);
  if (commentEnd > index) {
    return commentEnd;
  }
  return consumeQuotedOrBackticked(sql, index);
}

function consumeSqlComment(sql: string, index: number): number {
  if (sql[index] === "-" && sql[index + 1] === "-") {
    let i = index + 2;
    while (i < sql.length && sql[i] !== "\n") {
      i++;
    }
    return i;
  }
  if (sql[index] === "#") {
    let i = index + 1;
    while (i < sql.length && sql[i] !== "\n") {
      i++;
    }
    return i;
  }
  if (sql[index] === "/" && sql[index + 1] === "*") {
    let i = index + 2;
    while (i < sql.length && !(sql[i] === "*" && sql[i + 1] === "/")) {
      i++;
    }
    return i < sql.length ? i + 2 : sql.length;
  }
  return index;
}

function consumeQuotedOrBackticked(sql: string, index: number): number {
  if (sql[index] === "`") {
    let i = index + 1;
    while (i < sql.length) {
      if (sql[i] === "\\") {
        i += 2;
        continue;
      }
      if (sql[i] === "`") {
        if (sql[i + 1] === "`") {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return sql.length;
  }
  const stringStart = getQuotedStringStart(sql, index);
  if (stringStart !== -1) {
    return consumeQuotedString(sql, stringStart);
  }
  return index;
}

function getQuotedStringStart(sql: string, index: number): number {
  if (sql[index] === "'" || sql[index] === '"') {
    return index;
  }
  // Handle GoogleSQL raw/bytes string literal prefixes: r'...', b'...', rb'...', br'...'
  if (index === 0 || !/[A-Za-z0-9_$]/.test(sql[index - 1])) {
    const twoPrefix = sql.slice(index, index + 2).toLowerCase();
    if (
      (twoPrefix === "rb" || twoPrefix === "br") &&
      (sql[index + 2] === "'" || sql[index + 2] === '"')
    ) {
      return index + 2;
    }
    const onePrefix = (sql[index] || "").toLowerCase();
    if (
      (onePrefix === "r" || onePrefix === "b") &&
      (sql[index + 1] === "'" || sql[index + 1] === '"')
    ) {
      return index + 1;
    }
  }
  return -1;
}

function consumeQuotedString(sql: string, quoteIdx: number): number {
  const quoteChar = sql[quoteIdx];
  const tripleQuote = quoteChar + quoteChar + quoteChar;
  if (sql.slice(quoteIdx, quoteIdx + 3) === tripleQuote) {
    let i = quoteIdx + 3;
    while (i < sql.length) {
      if (sql[i] === "\\") {
        i += 2;
        continue;
      }
      if (sql.slice(i, i + 3) === tripleQuote) {
        return i + 3;
      }
      i++;
    }
    return sql.length;
  }

  let i = quoteIdx + 1;
  while (i < sql.length) {
    if (sql[i] === "\\") {
      i += 2;
      continue;
    }
    if (sql[i] === quoteChar) {
      return i + 1;
    }
    i++;
  }
  return sql.length;
}
