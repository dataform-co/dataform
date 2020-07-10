import { IInitResult } from "df/api/commands/init";
import { prettyJsonStringify } from "df/api/utils";
import { dataform } from "df/protos/ts";
import * as readlineSync from "readline-sync";

// Uses ANSI escape color codes.
// https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
const coloredOutput = (output: string, ansiColorCode: number) =>
  `\x1b[${ansiColorCode}m${output}\x1b[0m`;
const successOutput = (output: string) => coloredOutput(output, 32);
const warningOutput = (output: string) => coloredOutput(output, 93);
const errorOutput = (output: string) => coloredOutput(output, 91);
const calloutOutput = (output: string) => coloredOutput(output, 36);

const write = (stream: NodeJS.WriteStream, output: string, indentCount: number) =>
  stream.write(`${"  ".repeat(indentCount)}${output}\n`);
const writeStdOut = (output: string, indentCount: number = 0) =>
  write(process.stdout, output, indentCount);
const writeStdErr = (output: string, indentCount: number = 0) =>
  write(process.stderr, output, indentCount);

const DEFAULT_PROMPT = "> ";

export function question(questionText: string) {
  return prompt(questionText);
}

export function passwordQuestion(questionText: string) {
  return prompt(questionText, {
    hideEchoBack: true,
    mask: ""
  });
}

export function ynQuestion(questionText: string, defaultValue: boolean = false): boolean {
  const response = readlineSync.keyInYN(questionText);
  if (typeof response === "string") {
    return defaultValue;
  }
  return response;
}

export function intQuestion(questionText: string, defaultValue?: number) {
  return parseInt(
    prompt(questionText, {
      limit: value => {
        const intValue = parseInt(value, 10);
        return !isNaN(intValue);
      },
      limitMessage: errorOutput("Entered value must be an integer."),
      prompt: `[${defaultValue}] `,
      defaultInput: `${defaultValue}`
    }),
    10
  );
}

export function selectionQuestion(questionText: string, options: string[]) {
  return readlineSync.keyInSelect(options, `${questionText}\n`, {
    cancel: false
  });
}

function prompt(questionText: string, options?: readlineSync.BasicOptions) {
  writeStdOut(questionText);
  return readlineSync.prompt({
    ...options,
    prompt: (options && options.prompt && options.prompt + DEFAULT_PROMPT) || DEFAULT_PROMPT
  });
}

export function print(text: string) {
  writeStdOut(text);
}

export function printSuccess(text: string) {
  writeStdOut(successOutput(text));
}

export function printError(errorText: string, indentCount: number = 0) {
  writeStdErr(errorOutput(errorText), indentCount);
}

export function printInitResult(result: IInitResult) {
  if (result.dirsCreated && result.dirsCreated.length) {
    writeStdOut(successOutput("Directories successfully created:"));
    result.dirsCreated.forEach(dir => writeStdOut(dir, 1));
  }
  if (result.filesWritten && result.filesWritten.length) {
    writeStdOut(successOutput("Files successfully written:"));
    result.filesWritten.forEach(file => writeStdOut(file, 1));
  }
  if (result.installedNpmPackages) {
    writeStdOut(successOutput("NPM packages successfully installed."));
  }
}

export function printInitCredsResult(writtenFilePath: string) {
  writeStdOut(successOutput("Credentials file successfully written:"));
  writeStdOut(writtenFilePath, 1);
  writeStdOut("To change connection settings, edit this file directly.");
}

export function printCompiledGraph(graph: dataform.ICompiledGraph, verbose: boolean) {
  if (verbose) {
    writeStdOut(prettyJsonStringify(graph));
  } else {
    const actionCount =
      0 +
      (graph.tables ? graph.tables.length : 0) +
      (graph.assertions ? graph.assertions.length : 0) +
      (graph.operations ? graph.operations.length : 0);
    writeStdOut(successOutput(`Compiled ${actionCount} action(s).`));
    if (graph.tables && graph.tables.length) {
      writeStdOut(`${graph.tables.length} dataset(s):`);
      graph.tables.forEach(compiledTable => {
        writeStdOut(
          `${datasetString(compiledTable.target, compiledTable.type, compiledTable.disabled)}`,
          1
        );
      });
    }
    if (graph.assertions && graph.assertions.length) {
      writeStdOut(`${graph.assertions.length} assertion(s):`);
      graph.assertions.forEach(assertion => {
        writeStdOut(assertionString(assertion.target, assertion.disabled), 1);
      });
    }
    if (graph.operations && graph.operations.length) {
      writeStdOut(`${graph.operations.length} operation(s):`);
      graph.operations.forEach(operation => {
        writeStdOut(operationString(operation.target, operation.disabled), 1);
      });
    }
  }
}

export function printCompiledGraphErrors(graphErrors: dataform.IGraphErrors) {
  if (graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0) {
    printError("Compilation errors:", 1);
    graphErrors.compilationErrors.forEach(compileError => {
      writeStdErr(
        `${calloutOutput(compileError.fileName)}: ${errorOutput(
          compileError.stack || compileError.message
        )}`,
        1
      );
    });
  }
}

export function printTestResult(testResult: dataform.ITestResult) {
  writeStdOut(
    `${testResult.name}: ${testResult.successful ? successOutput("passed") : errorOutput("failed")}`
  );
  if (!testResult.successful) {
    testResult.messages.forEach(message => writeStdErr(message, 1));
  }
}

export function printExecutionGraph(executionGraph: dataform.ExecutionGraph, verbose: boolean) {
  if (verbose) {
    writeStdOut(prettyJsonStringify(executionGraph.toJSON()));
  } else {
    const actionsByType = {
      table: [] as dataform.IExecutionAction[],
      assertion: [] as dataform.IExecutionAction[],
      operation: [] as dataform.IExecutionAction[]
    };
    executionGraph.actions.forEach(action => {
      if (
        !(action.type === "table" || action.type === "assertion" || action.type === "operation")
      ) {
        throw new Error(`Unrecognized action type: ${action.type}`);
      }
      actionsByType[action.type].push(action);
    });
    const datasetActions = actionsByType.table;
    if (datasetActions && datasetActions.length) {
      writeStdOut(`${datasetActions.length} dataset(s):`);
      datasetActions.forEach(datasetAction =>
        writeStdOut(
          datasetString(datasetAction.target, datasetAction.type, datasetAction.tasks.length === 0),
          1
        )
      );
    }
    const assertionActions = actionsByType.assertion;
    if (assertionActions && assertionActions.length) {
      writeStdOut(`${assertionActions.length} assertion(s):`);
      assertionActions.forEach(assertionAction =>
        writeStdOut(assertionString(assertionAction.target, assertionAction.tasks.length === 0), 1)
      );
    }
    const operationActions = actionsByType.operation;
    if (operationActions && operationActions.length) {
      writeStdOut(`${operationActions.length} operation(s):`);
      operationActions.forEach(operationAction =>
        writeStdOut(operationString(operationAction.target, operationAction.tasks.length === 0), 1)
      );
    }
  }
}

export function printExecutedAction(
  executedAction: dataform.IActionResult,
  executionAction: dataform.IExecutionAction
) {
  switch (executedAction.status) {
    case dataform.ActionResult.ExecutionStatus.SUCCESSFUL: {
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${successOutput("Dataset created: ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${successOutput("Assertion passed: ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${successOutput("Operation completed successfully: ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
      }
    }
    case dataform.ActionResult.ExecutionStatus.FAILED: {
      switch (executionAction.type) {
        case "table": {
          writeStdErr(
            `${errorOutput("Dataset creation failed: ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}`
          );
          break;
        }
        case "assertion": {
          writeStdErr(
            `${errorOutput("Assertion failed: ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          break;
        }
        case "operation": {
          writeStdErr(
            `${errorOutput("Operation failed: ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          break;
        }
      }
      printExecutedActionErrors(executedAction, executionAction);
      return;
    }
    case dataform.ActionResult.ExecutionStatus.SKIPPED: {
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Skipping dataset creation: ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput("Skipping assertion execution: ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput("Skipping operation execution: ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
      }
      return;
    }
    case dataform.ActionResult.ExecutionStatus.DISABLED: {
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Dataset creation disabled: ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput(`Assertion execution disabled: `)} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput(`Operation execution disabled: `)} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
      }
      return;
    }
    case dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED: {
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Skipped dataset creation (cache hit): ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput("Skipped assertion execution (cache hit): ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput("Skipped operation execution (cache hit): ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}`
          );
          return;
        }
      }
      return;
    }
  }
}

export function printFormatFilesResult(
  formatResults: Array<{
    filename: string;
    err?: Error;
  }>
) {
  const sorted = formatResults.sort((a, b) => a.filename.localeCompare(b.filename));
  const successfulFormatResults = sorted.filter(result => !result.err);
  const failedFormatResults = sorted.filter(result => !!result.err);
  if (successfulFormatResults.length > 0) {
    printSuccess("Successfully formatted:");
    successfulFormatResults.forEach(result => writeStdOut(result.filename, 1));
  }
  if (failedFormatResults.length > 0) {
    printError("Errors encountered during formatting:");
    failedFormatResults.forEach(result =>
      writeStdOut(`${result.filename}: ${result.err.message}`, 1)
    );
  }
}

export function printListTablesResult(tables: dataform.ITarget[]) {
  tables.forEach(foundTable => writeStdOut(`${foundTable.schema}.${foundTable.name}`));
}

export function printGetTableResult(tableMetadata: dataform.ITableMetadata) {
  writeStdOut(prettyJsonStringify(tableMetadata));
}

function datasetString(target: dataform.ITarget, datasetType: string, disabled: boolean) {
  return `${targetString(target)} [${datasetType}]${disabled ? " [disabled]" : ""}`;
}

function assertionString(target: dataform.ITarget, disabled: boolean) {
  return `${targetString(target)}${disabled ? " [disabled]" : ""}`;
}

function operationString(target: dataform.ITarget, disabled: boolean) {
  return `${targetString(target)}${disabled ? " [disabled]" : ""}`;
}

function targetString(target: dataform.ITarget) {
  return calloutOutput(`${target.schema}.${target.name}`);
}

function printExecutedActionErrors(
  executedAction: dataform.IActionResult,
  executionAction: dataform.IExecutionAction
) {
  const failingTasks = executedAction.tasks.filter(
    task => task.status === dataform.ActionResult.ExecutionStatus.FAILED
  );
  failingTasks.forEach((task, i) => {
    executionAction.tasks[i].statement.split("\n").forEach(line => {
      writeStdErr(`${DEFAULT_PROMPT}${line}`, 1);
    });
    printError(task.errorMessage, 1);
  });
}
