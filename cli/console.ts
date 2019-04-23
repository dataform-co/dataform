import { InitResult } from "@dataform/api/commands/init";
import { prettyJsonStringify } from "@dataform/api/utils";
import { dataform } from "@dataform/protos";
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

export const question = (questionText: string) => prompt(questionText);

export const passwordQuestion = (questionText: string) =>
  prompt(questionText, {
    hideEchoBack: true,
    mask: ""
  });

export const intQuestion = (questionText: string, defaultValue?: number) =>
  parseInt(
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

export const print = (text: string) => writeStdOut(text);

export function printSuccess(text: string) {
  writeStdOut(successOutput(text));
}

export function printError(errorText: string, indentCount: number = 0) {
  writeStdErr(errorOutput(errorText), indentCount);
}

export function printInitResult(result: InitResult) {
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
    writeStdOut(successOutput(`Compiled ${actionCount} actions.`));
    if (graph.tables && graph.tables.length) {
      writeStdOut(`${graph.tables.length} dataset(s):`);
      graph.tables.forEach(compiledTable => {
        writeStdOut(
          `${datasetString(compiledTable.target, compiledTable.type)}${
            compiledTable.disabled ? " [disabled]" : ""
          }`,
          1
        );
      });
    }
    if (graph.assertions && graph.assertions.length) {
      writeStdOut(`${graph.assertions.length} assertion(s):`);
      graph.assertions.forEach(assertion => {
        writeStdOut(targetString(assertion.target), 1);
      });
    }
    if (graph.operations && graph.operations.length) {
      writeStdOut(`${graph.operations.length} operation(s):`);
      graph.operations.forEach(operation => {
        writeStdOut(targetString(operation.target), 1);
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
  if (graphErrors.validationErrors && graphErrors.validationErrors.length > 0) {
    printError("Validation errors:");
    graphErrors.validationErrors.forEach(validationError => {
      writeStdErr(
        `${calloutOutput(validationError.nodeName)}: ${errorOutput(validationError.message)}`,
        1
      );
    });
  }
}

export function printExecutionGraph(executionGraph: dataform.IExecutionGraph, verbose: boolean) {
  if (verbose) {
    writeStdOut(prettyJsonStringify(executionGraph));
  } else {
    const nodesByType = {
      table: [],
      assertion: [],
      operation: []
    };
    executionGraph.nodes.forEach(node => {
      nodesByType[node.type].push(node);
    });
    const datasetNodes = nodesByType.table;
    if (datasetNodes && datasetNodes.length) {
      writeStdOut(`${datasetNodes.length} dataset(s):`);
      datasetNodes.forEach(datasetNode =>
        writeStdOut(datasetString(datasetNode.target, datasetNode.type), 1)
      );
    }
    const assertionNodes = nodesByType.assertion;
    if (assertionNodes && assertionNodes.length) {
      writeStdOut(`${assertionNodes.length} assertion(s):`);
      assertionNodes.forEach(assertionNode => writeStdOut(targetString(assertionNode.target), 1));
    }
    const operationNodes = nodesByType.operation;
    if (operationNodes && operationNodes.length) {
      writeStdOut(`${operationNodes.length} operation(s):`);
      operationNodes.forEach(operationNode => writeStdOut(targetString(operationNode.target), 1));
    }
  }
}

export function printExecutedNode(
  executedNode: dataform.IExecutedNode,
  executionNode: dataform.IExecutionNode,
  verbose: boolean
) {
  switch (executedNode.status) {
    case dataform.NodeExecutionStatus.SUCCESSFUL: {
      switch (executionNode.type) {
        case "table": {
          writeStdOut(
            `${successOutput("Dataset created: ")} ${datasetString(
              executionNode.target,
              executionNode.tableType
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${successOutput(`Assertion passed: `)} ${targetString(executionNode.target)}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${successOutput(`Operation completed successfully: `)} ${targetString(
              executionNode.target
            )}`
          );
          return;
        }
      }
    }
    case dataform.NodeExecutionStatus.FAILED: {
      switch (executionNode.type) {
        case "table": {
          writeStdErr(
            `${errorOutput("Dataset creation failed: ")} ${datasetString(
              executionNode.target,
              executionNode.tableType
            )}`
          );
          break;
        }
        case "assertion": {
          writeStdErr(`${errorOutput(`Assertion failed: `)} ${targetString(executionNode.target)}`);
          break;
        }
        case "operation": {
          writeStdErr(`${errorOutput(`Operation failed: `)} ${targetString(executionNode.target)}`);
          break;
        }
      }
      printExecutedNodeErrors(executedNode, verbose);
      return;
    }
    case dataform.NodeExecutionStatus.SKIPPED: {
      switch (executionNode.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Skipping dataset creation: ")} ${datasetString(
              executionNode.target,
              executionNode.tableType
            )}`
          );
          return;
        }
        case "assertion":
        case "operation": {
          writeStdOut(
            `${warningOutput(`Skipping ${executionNode.type} execution: `)} ${targetString(
              executionNode.target
            )}`
          );
          return;
        }
      }
      return;
    }
    case dataform.NodeExecutionStatus.DISABLED: {
      switch (executionNode.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Dataset creation disabled: ")} ${datasetString(
              executionNode.target,
              executionNode.tableType
            )}`
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput(`Assertion execution disabled: `)} ${targetString(
              executionNode.target
            )}`
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput(`Operation execution disabled: `)} ${targetString(
              executionNode.target
            )}`
          );
          return;
        }
      }
      return;
    }
  }
}

export function printListTablesResult(tables: dataform.ITarget[]) {
  tables.forEach(foundTable => writeStdOut(`${foundTable.schema}.${foundTable.name}`));
}

export function printGetTableResult(tableMetadata: dataform.ITableMetadata) {
  writeStdOut(prettyJsonStringify(tableMetadata));
}

function datasetString(target: dataform.ITarget, datasetType: string) {
  return `${targetString(target)} [${datasetType}]`;
}

function targetString(target: dataform.ITarget) {
  return calloutOutput(`${target.schema}.${target.name}`);
}

function printExecutedNodeErrors(executedNode: dataform.IExecutedNode, verbose: boolean) {
  const failingTasks = executedNode.tasks.filter(task => !task.ok);
  failingTasks.forEach(task => {
    task.task.statement.split("\n").forEach(line => {
      writeStdErr(`${DEFAULT_PROMPT}${line}`, 1);
    });
    printError(task.error, 1);
  });
}
