import { IInitResult } from "df/cli/api/commands/init";
import { prettyJsonStringify } from "df/cli/api/utils";
import { formatBytesInHumanReadableFormat, formatExecutionSuffix } from "df/cli/util";
import { setOrValidateTableEnumType, tableTypeEnumToString } from "df/core/utils";
import { dataform } from "df/protos/ts";
import * as readlineSync from "readline-sync";

// Support disabling colors in CLI output by using informal standard from https://no-color.org/
// NO_COLOR=1, NO_COLOR=true, NO_COLOR=yes
const noColor =
  process.env.NO_COLOR && ["1", "true", "yes"].includes(process.env.NO_COLOR.toLowerCase());

const ansiColorCodes = Object.freeze({
  red: 91,
  green: 32,
  yellow: 93,
  cyan: 36
});

function output(text: string, ansiColorCode: number): string {
  if (noColor) {
    return text;
  }

  // Uses ANSI escape color codes.
  // https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
  return `\x1b[${ansiColorCode}m${text}\x1b[0m`;
}

const successOutput = (text: string) => output(text, ansiColorCodes.green);
const warningOutput = (text: string) => output(text, ansiColorCodes.yellow);
const errorOutput = (text: string) => output(text, ansiColorCodes.red);
const calloutOutput = (text: string) => output(text, ansiColorCodes.cyan);

const write = (stream: NodeJS.WriteStream, text: string, indentCount: number) =>
  stream.write(`${"  ".repeat(indentCount)}${text}\n`);
const writeStdOut = (text: string, indentCount: number = 0) =>
  write(process.stdout, text, indentCount);
const writeStdErr = (text: string, indentCount: number = 0) =>
  write(process.stderr, text, indentCount);

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
}

export function printInitCredsResult(writtenFilePath: string) {
  writeStdOut(successOutput("Credentials file successfully written:"));
  writeStdOut(writtenFilePath, 1);
  writeStdOut("To change connection settings, edit this file directly.");
}

export function printCompiledGraph(graph: dataform.ICompiledGraph, asJson: boolean, quietCompilation: boolean) {
  if (asJson) {
    writeStdOut(prettyJsonStringify(graph));
  } else {
    const actionCount =
      0 +
      (graph.tables ? graph.tables.length : 0) +
      (graph.assertions ? graph.assertions.length : 0) +
      (graph.operations ? graph.operations.length : 0);
    writeStdOut(successOutput(`Compiled ${actionCount} action(s).`));
    if (graph.tables && graph.tables.length) {
      graph.tables.forEach(setOrValidateTableEnumType);
      writeStdOut(`${graph.tables.length} dataset(s)${quietCompilation ? "." : ":"}`);
      if(!quietCompilation){
          graph.tables.forEach(compiledTable => {
            writeStdOut(
              `${datasetString(
                compiledTable.target,
                tableTypeEnumToString(compiledTable.enumType),
                compiledTable.disabled
              )}`,
              1
            );
          });
      }
    }
    if (graph.assertions && graph.assertions.length) {
      writeStdOut(`${graph.assertions.length} assertion(s)${quietCompilation ? "." : ":"}`);
      if(!quietCompilation){
          graph.assertions.forEach(assertion => {
            writeStdOut(assertionString(assertion.target, assertion.disabled), 1);
          });
      }
    }
    if (graph.operations && graph.operations.length) {
      writeStdOut(`${graph.operations.length} operation(s)${quietCompilation ? "." : ":"}`);
      if(!quietCompilation){
          graph.operations.forEach(operation => {
            writeStdOut(operationString(operation.target, operation.disabled), 1);
          });
      }
    }
  }
}

function formatStackTraceForQuietCompilation(compileError: dataform.ICompilationError): string {
  // Show only first 3 or available lines for cleaner error output
  // which contains the information on the file where the error occurred and the sufficient metadata for the user to fix the error. For e.g.
  // (line: 1) Unexpected identifier <file_path>: <line_number>
  // (line: 2) tags: ["<tag_name>"]
  // (line: 3) ^^^^
  if (compileError?.message?.includes("Unexpected identifier")) {
    if (!compileError.stack) {
      return "";
    }
    const stackLines = compileError.stack?.split("\n") || [];
    return stackLines.slice(0, Math.min(3, stackLines.length)).join("\n");
  }
  return "";
}

export function printCompiledGraphErrors(graphErrors: dataform.IGraphErrors, quietCompilation: boolean) {
  if (graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0) {
    printError("Compilation errors:", 1);
    graphErrors.compilationErrors.forEach(compileError => {
      writeStdErr(
        `${calloutOutput(compileError.fileName)}: ${errorOutput(
          quietCompilation ? (compileError.message + " " + formatStackTraceForQuietCompilation(compileError) || compileError.stack) : (compileError.stack || compileError.message)
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

export function printExecutionGraph(executionGraph: dataform.ExecutionGraph, asJson: boolean) {
  if (asJson) {
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
  executionAction: dataform.IExecutionAction,
  dryRun?: boolean
) {
  const jobIds = executedAction.tasks
    .filter(task => task.metadata?.bigquery?.jobId)
    .map(task => task.metadata.bigquery.jobId);
  const bytesBilled = executedAction.tasks
    .filter(task => task.metadata?.bigquery?.jobId)
    .map(task => {
        const bytes = task.metadata.bigquery?.totalBytesBilled?.toNumber?.() ?? 0;
        return formatBytesInHumanReadableFormat(bytes);
    });

  const executionSuffix = formatExecutionSuffix(jobIds, bytesBilled);

  switch (executedAction.status) {
    case dataform.ActionResult.ExecutionStatus.SUCCESSFUL: {
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${successOutput(`Table ${dryRun ? "dry run success" : "created"}: `)} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0
            )}${executionSuffix}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${successOutput(
              `Assertion ${dryRun ? "dry run success" : "passed"}: `
            )} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}${executionSuffix}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${successOutput(
              `Operation ${dryRun ? "dry run success" : "completed successfully"}: `
            )} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}${executionSuffix}`
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
            )}${executionSuffix}`
          );
          break;
        }
        case "assertion": {
          writeStdErr(
            `${errorOutput("Assertion failed: ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}${executionSuffix}`
          );
          break;
        }
        case "operation": {
          writeStdErr(
            `${errorOutput("Operation failed: ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0
            )}${executionSuffix}`
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
  }
}

export function printFormatFilesResult(
  formatResults: Array<{
    filename: string;
    err?: Error;
    needsFormatting?: boolean;
  }>
) {
  const sorted = formatResults.sort((a, b) => a.filename.localeCompare(b.filename));
  const successfulFormatResults = sorted.filter(result => !result.err && !result.needsFormatting);
  const needsFormattingResults = sorted.filter(result => !result.err && result.needsFormatting);
  const failedFormatResults = sorted.filter(result => !!result.err);

  if (successfulFormatResults.length > 0) {
    printSuccess("Successfully formatted:");
    successfulFormatResults.forEach(result => writeStdOut(result.filename, 1));
  }

  if (needsFormattingResults.length > 0) {
    printError("Files that need formatting:");
    needsFormattingResults.forEach(result => writeStdErr(result.filename, 1));
  }

  if (failedFormatResults.length > 0) {
    printError("Errors encountered during formatting:");
    failedFormatResults.forEach(result =>
      writeStdOut(`${result.filename}: ${result.err.message}`, 1)
    );
  }
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
    task => task.status === dataform.TaskResult.ExecutionStatus.FAILED
  );
  failingTasks.forEach((task, i) => {
    executionAction.tasks[i].statement.split("\n").forEach(line => {
      writeStdErr(`${DEFAULT_PROMPT}${line}`, 1);
    });
    printError(task.errorMessage, 1);
  });
}
