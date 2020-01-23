interface IErrorLocation {
  line?: number;
  column?: number;
}

interface IAzureEvalError {
  originalError?: {
    info?: {
      message?: string;
    };
  };
  errorLocation?: IErrorLocation;
}

export const AzureEvalErrorParser = (error: IAzureEvalError) => {
  // expected error format:
  // // Parse error at line: 14, column: 13: Incorrect syntax near 'current_date'.
  if (error.originalError && error.originalError.info && error.originalError.info.message) {
    const message = error.originalError.info.message;
    // extract all characters between line: and , inclusive
    const lineMatch = message.match(/line:(.*?),/g);
    // if we get more than one match, bail out
    if (lineMatch.length !== 1) {
      return error;
    }
    // get rid of `line: `
    const lineLess = lineMatch[0].replace(/[line: ]/g, "");
    // get rid of trailing comma
    const lineNumber = lineLess.replace(/[,]/g, "");

    // extract all characters between column: and , inclusive
    const columnMatch = message.match(/column:(.*?):/g);
    // if we get more than one match, bail out, but keep the line number
    if (columnMatch.length !== 1) {
      error.errorLocation = { line: Number(lineNumber) };
      return error;
    }
    // get rid of `column: `
    const columnLess = columnMatch[0].replace(/[column: ]/g, "");
    // get rid of trailing :
    const columnNumber = columnLess.replace(/[:]/g, "");

    error.errorLocation = { line: Number(lineNumber), column: Number(columnNumber) };
  }
  return error;
};

interface IRedshiftEvalError {
  message: string;
  position?: string;
  errorLocation?: IErrorLocation;
}

export const RedshiftEvalErrorParser = (statement: string, error: IRedshiftEvalError) => {
  // expected error format:
  // // e.position = "123" - position is the number of characters into the query that the error was found at
  // // including \n characters

  if (!error.position) {
    return error;
  }
  // split statement into lines
  const statementSplitByLine = statement.split("\n");
  // remove the part of the statement before the error position
  const statementAfterError = statement.substring(Number(error.position) - 1);
  // split what's left by line
  const statementAfterErrorSplit = statementAfterError.split("\n");
  // original length - the statement after error is the index of the error
  const errorIndex = statementSplitByLine.length - statementAfterErrorSplit.length;

  // difference + 1 for zero-indexing
  error.errorLocation = { line: errorIndex + 1 };
  return error;
};

interface IBigqueryEvalError {
  message?: string;
  errorLocation?: IErrorLocation;
}

export const BigqueryEvalErrorParser = (error: IBigqueryEvalError) => {
  // expected error format:
  // // e.message = Syntax error: Unexpected identifier "asda" at [2:1]
  if (!error.message) {
    return error;
  }

  // extract all characters between '[' and ']' (inclusive)
  const matches = error.message.match(/\[(.*?)\]/g);
  // ensure that only one bracket has been extracted
  // if so, we can't parse this properly
  if (matches.length > 1) {
    return error;
  }
  // get rid of the brackets []
  const errorLocation = matches[0].replace(/[\[\]]+/g, "");
  const [line, column] = errorLocation.split(":");
  error.errorLocation = { line: Number(line), column: Number(column) };
  return error;
};
