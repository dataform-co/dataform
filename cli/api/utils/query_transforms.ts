/**
 * Whether the string or bytes literal opening quote (`'` or `"`) at `quoteIndex` is preceded by a
 * GoogleSQL raw literal prefix (`r`, `rb` or `br`, case-insensitive).
 */
function hasRawLiteralPrefix(statement: string, quoteIndex: number): boolean {
  const prefix = statement.slice(Math.max(0, quoteIndex - 3), quoteIndex);
  return /(?:^|[^a-zA-Z0-9_])(?:r|rb|br)$/i.test(prefix);
}

/**
 * Returns `statement` with comments (`--`, `#` and `/* *\/`) and the contents of string/bytes
 * literals (single, double and triple quoted, including raw literals) and quoted identifiers
 * replaced with spaces of the same length. Preserving lengths keeps character indices aligned
 * with `statement`, while leaving only unquoted SQL tokens and empty quotes.
 */
function stripCommentsAndQuotedContent(statement: string): string {
  let result = "";
  let i = 0;
  while (i < statement.length) {
    const char = statement[i];
    const next = statement[i + 1];
    if ((char === "-" && next === "-") || char === "#") {
      const newline = statement.indexOf("\n", i);
      const end = newline === -1 ? statement.length : newline;
      result += " ".repeat(end - i);
      i = end;
    } else if (char === "/" && next === "*") {
      const close = statement.indexOf("*/", i + 2);
      const end = close === -1 ? statement.length : close + 2;
      result += " ".repeat(end - i);
      i = end;
    } else if (char === "'" || char === '"' || char === "`") {
      const isRaw = char !== "`" && hasRawLiteralPrefix(statement, i);
      const tripleQuote = char.repeat(3);
      const closing = char !== "`" && statement.startsWith(tripleQuote, i) ? tripleQuote : char;
      const start = i;
      i += closing.length;
      while (i < statement.length && !statement.startsWith(closing, i)) {
        // Skip escaped characters, e.g. \' inside a non-raw single-quoted string.
        i += !isRaw && statement[i] === "\\" ? 2 : 1;
      }
      i = Math.min(i + closing.length, statement.length);
      result += char + " ".repeat(Math.max(0, i - start - 2)) + (i - start > 1 ? char : "");
    } else {
      result += char;
      i++;
    }
  }
  return result;
}

/**
 * Returns the index just after the last character of code in the given GoogleSQL statement, i.e.
 * ignoring trailing whitespace and comments (`--`, `#` and `/* *\/`). String literals (single,
 * double and triple quoted) and quoted identifiers are skipped as a whole, so that e.g. a `--`
 * inside a string literal is not mistaken for a comment. Returns 0 if the statement has no code.
 */
function codeEndIndex(statement: string): number {
  return stripCommentsAndQuotedContent(statement).trimEnd().length;
}

function formatStatement(
  statement: string,
  isLast: boolean,
  modifier?: (mod: string) => string,
  codeEnd: number = codeEndIndex(statement),
) {
  // Split off trailing comments, so that the modifier and the terminating semicolon only apply to
  // the code: a semicolon after a trailing `--` or `#` comment would be commented out.
  let code = statement.substring(0, codeEnd);
  const comment = statement.substring(codeEnd);

  if (modifier && code.length > 0) {
    code = modifier(code);
  }

  if (!isLast && code.length > 0 && !code.endsWith(";")) {
    code = code + ";";
  }

  return code + comment;
}

export function concatenateQueries(
  statements: string[],
  modifier?: (mod: string) => string,
  trimLeadingWhitespace = true,
) {
  const processed = statements
    .filter((statement) => !!statement)
    .map((statement) => (trimLeadingWhitespace ? statement.trim() : statement.trimEnd()))
    .filter((statement) => statement.length > 0)
    .map((statement) => ({ statement, codeEnd: codeEndIndex(statement) }));
  let lastCodeIndex = -1;
  processed.forEach(({ codeEnd }, index) => {
    if (codeEnd > 0) {
      lastCodeIndex = index;
    }
  });
  return processed
    .map(({ statement, codeEnd }, index) =>
      formatStatement(statement, index >= lastCodeIndex, modifier, codeEnd),
    )
    .join("\n");
}

/**
 * Number of lines `wrapWithCreateSchemaIfNotExists()` adds before the wrapped statement: the
 * statement starts on line `1 + CREATE_SCHEMA_WRAPPER_LINE_OFFSET` of the script.
 */
export const CREATE_SCHEMA_WRAPPER_LINE_OFFSET = 2;

/**
 * Wraps the given statement (or script) so that the `database.schema` dataset is created, if it
 * doesn't exist yet, in the same BigQuery job. This lets BigQuery infer the location of the new
 * dataset from the tables read by the statement.
 *
 * The statement is wrapped in a `BEGIN ... END` block, rather than just prepended with the
 * `CREATE SCHEMA` statement, because `DECLARE` statements are only allowed at the start of a
 * script or block.
 */
export function wrapWithCreateSchemaIfNotExists(
  database: string | undefined,
  schema: string,
  statement: string,
): string {
  const schemaRef = database ? `${database}.${schema}` : schema;
  const terminatedStatement = formatStatement(statement.trim(), false);
  return `create schema if not exists \`${schemaRef}\`;\nbegin\n${terminatedStatement}\nend;`;
}

/**
 * Whether the given GoogleSQL statement or script sets the `@@dataset_id` system variable (e.g.
 * `SET @@dataset_id = '...'`), ignoring comments, string literals, and quoted identifiers.
 */
export function setsDatasetId(statement: string): boolean {
  return (
    /@@dataset_id\b/i.test(statement) &&
    /(?:^|[^a-zA-Z0-9_@$])set\s+(?:@@dataset_id\b|\([^;)=]*@@dataset_id\b)/i.test(
      stripCommentsAndQuotedContent(statement),
    )
  );
}
