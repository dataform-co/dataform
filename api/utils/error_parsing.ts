import { dataform } from "df/protos/ts";

interface IBigqueryEvaluationError {
  message?: string;
}

export function parseBigqueryEvalError(error: IBigqueryEvaluationError) {
  // expected error format:
  // e.message = Syntax error: Unexpected identifier "asda" at [2:1]
  const evalError = dataform.QueryEvaluationError.create({
    message: String(error)
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
