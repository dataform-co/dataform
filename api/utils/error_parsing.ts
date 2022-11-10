import { dataform } from "df/protos/ts";

interface IAzureEvaluationError {
  originalError?: {
    info?: {
      message?: string;
    };
  };
}

export function parseAzureEvaluationError(error: IAzureEvaluationError) {
  // expected error format:
  // Parse error at line: 14, column: 13: Incorrect syntax near 'current_date'.
  const evalError = dataform.QueryEvaluationError.create({
    message: String(error)
  });
  try {
    if (error.originalError && error.originalError.info && error.originalError.info.message) {
      const message = error.originalError.info.message;
      evalError.message = message;

      // extract line and column number
      // assumes the first two numbers in the message are line followed by column
      const [_, lineNumber, columnNumber] = message.match(/[^0-9]*([0-9]*)[^0-9]*([0-9]*).*/);

      evalError.errorLocation = { line: Number(lineNumber), column: Number(columnNumber) };
      return evalError;
    }
  } catch (_) {
    // Do nothing.
  }
  return evalError;
}

interface IRedshiftEvaluationError {
  position?: string;
}

export function parseRedshiftEvalError(statement: string, error: IRedshiftEvaluationError) {
  // expected error format:
  // e.position = "123" - position is the number of characters into the query that the error was found at
  // including \n characters

  const evalError = dataform.QueryEvaluationError.create({
    message: String(error)
  });
  try {
    if (!error.position) {
      return evalError;
    }

    const statementBeforeError = statement.substring(0, Number(error.position));
    const lineNumber = statementBeforeError.split("\n").length;
    const lineIncludingError = statementBeforeError.split("\n").slice(-1)[0];
    const colNumber = lineIncludingError.length + 1;
    evalError.errorLocation = { line: lineNumber, column: colNumber };
  } catch (_) {
    // Do nothing.
  }

  return evalError;
}

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

export function parseSnowflakeEvalError(error: string) {
  const evalError = dataform.QueryEvaluationError.create({
    message: error
  });
  try {
    const regex = /line ([0-9]+) at position ([0-9]+)/;
    const match = error.match(regex);
    if (!match) {
      // Do nothing.
    }
    evalError.errorLocation = {
      line: Number(match[1]),
      column: Number(match[2]) + 1
    };
  } catch (e) {
    // Do nothing.
  }
  return evalError;
}
