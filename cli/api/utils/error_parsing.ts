import { dataform } from "df/protos/ts";

interface IBigqueryEvaluationError {
  message?: string;
}

export function parseBigqueryEvalError(error: IBigqueryEvaluationError) {
  // expected error format:
  // e.message = Syntax error: Unexpected identifier "asda" at [2:1]
  const evalError = dataform.QueryEvaluationError.create({
    message: String(error),
  });
  try {
    if (!error.message) {
      return evalError;
    }

    // extract everything after the very last [ in the string
    const bracketsString = error.message.split("[").slice(-1)[0];
    const [_, lineNumber, columnNumber] = bracketsString.match(/([0-9]*)[^0-9]*([0-9]*).*/);
    const line = Number(lineNumber);
    const column = Number(columnNumber);
    if (line || line === 0) {
      // Column defaults to 0 if not found.
      evalError.errorLocation = { line, column };
    }
  } catch (_) {
    // Do nothing.
  }
  return evalError;
}

/**
 * Maps the `at [line:column]` positions of a BigQuery error in `query` to `statement`, which has
 * the same lines as `query` shifted down by `lineOffset`, except that a line of `statement` may
 * have a prefix before the corresponding line of `query` (e.g. `create or replace table … as `
 * before the first line of a query that doesn't start with a line break).
 */
export function mapErrorPositions(
  message: string,
  query: string,
  statement: string,
  lineOffset: number,
): string {
  const queryLines = query.split("\n");
  const statementLines = statement.split("\n");
  return message.replace(/at \[(\d+):(\d+)\]/g, (_, lineString: string, columnString: string) => {
    const line = Number(lineString);
    const columnOffset = prefixLength(
      statementLines[line - 1 + lineOffset] ?? "",
      queryLines[line - 1] ?? "",
    );
    return `at [${line + lineOffset}:${Number(columnString) + columnOffset}]`;
  });
}

/**
 * Returns the length of the prefix before `queryLine` in `statementLine`, or 0 if there's none. The
 * rest of `statementLine` may also have one more `;` than `queryLine`, terminating the statement
 * (e.g. when it's wrapped in a script).
 */
function prefixLength(statementLine: string, queryLine: string): number {
  // Without an added semicolon.
  let prefix = statementLine.length - queryLine.length;
  if (prefix > 0 && statementLine.endsWith(queryLine)) {
    return prefix;
  }
  // With an added semicolon somewhere in the rest.
  prefix -= 1;
  if (prefix > 0) {
    const rest = statementLine.slice(prefix);
    for (let i = rest.indexOf(";"); i >= 0; i = rest.indexOf(";", i + 1)) {
      if (rest.slice(0, i) + rest.slice(i + 1) === queryLine) {
        return prefix;
      }
    }
  }
  return 0;
}
