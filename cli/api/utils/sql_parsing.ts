/**
 * Returns true if the last significant (non-whitespace, non-comment) character of the given
 * GoogleSQL statement is a `;` statement terminator. Comments (`--`, `#`, `/* *\/`), string
 * literals (single, double and triple quoted) and quoted identifiers are skipped, so that e.g.
 * a `--` or `;` inside a string literal is not mistaken for a comment or a terminator.
 */
export function endsWithStatementTerminator(statement: string): boolean {
  let lastSignificantChar = "";
  let i = 0;
  while (i < statement.length) {
    const char = statement[i];
    const next = statement[i + 1];
    if ((char === "-" && next === "-") || char === "#") {
      const newline = statement.indexOf("\n", i);
      i = newline === -1 ? statement.length : newline + 1;
    } else if (char === "/" && next === "*") {
      const end = statement.indexOf("*/", i + 2);
      i = end === -1 ? statement.length : end + 2;
    } else if (char === "'" || char === '"' || char === "`") {
      const tripleQuote = char.repeat(3);
      const isTriple = char !== "`" && statement.startsWith(tripleQuote, i);
      const closing = isTriple ? tripleQuote : char;
      i += closing.length;
      while (i < statement.length && !statement.startsWith(closing, i)) {
        i += statement[i] === "\\" ? 2 : 1;
      }
      i += closing.length;
      lastSignificantChar = char;
    } else {
      if (!/\s/.test(char)) {
        lastSignificantChar = char;
      }
      i++;
    }
  }
  return lastSignificantChar === ";";
}
