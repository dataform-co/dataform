import * as crypto from "crypto";
import * as readlineSync from "readline-sync";

import { IInitResult } from "df/cli/api/commands/init";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  formatBytesInHumanReadableFormat,
  formatDatasetWarehouseState,
  formatExecutionSuffix,
} from "df/cli/util";
import { targetStringifier } from "df/core/targets";
import { setOrValidateTableEnumType, tableTypeEnumToString } from "df/core/utils";
import { dataform } from "df/protos/ts";

// Support disabling colors in CLI output by using informal standard from https://no-color.org/
// NO_COLOR=1, NO_COLOR=true, NO_COLOR=yes
const noColor =
  process.env.NO_COLOR && ["1", "true", "yes"].includes(process.env.NO_COLOR.toLowerCase());

const ansiColorCodes = Object.freeze({
  red: 91,
  green: 32,
  yellow: 93,
  cyan: 36,
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

export class Logger {
  constructor(private readonly shouldLog: boolean) {}

  public log(text: string) {
    if (this.shouldLog) {
      print(text);
    }
  }

  public success(text: string) {
    if (this.shouldLog) {
      printSuccess(text);
    }
  }
}

export function question(questionText: string) {
  return prompt(questionText);
}

/**
 * Test-friendly function that prompts the user for input. If running in a
 * non-TTY (i.e. test) environment and DATAFORM_CLI_TEST_INPUTS is set,
 * it consumes inputs from the environment variable.
 * @param questionText The question text to display.
 * @returns Users's input or test input.
 */
export function interactiveQuestion(questionText: string): string {
  // If running in a non-TTY environment and test inputs are provided
  if (!process.stdin.isTTY && process.env.DATAFORM_CLI_TEST_INPUTS !== undefined) {
    const testInput = getTestInput(questionText);
    print(`${questionText} ${testInput}\n`); // Echo the test input for clarity in logs
    return testInput;
  }

  // If running in TTY environment or if testInputs are not available in non-TTY
  print(`${questionText} `);
  return readlineSync.question("");
}

/**
 * Helper function to enable testing interactive CLI. Retrieves testInput from
 * the DATAFORM_CLI_TEST_INPUTS environment variable.
 * @param questionText The exact question text displayed to the user.
 * @returns Test input for the provided question.
 */
function getTestInput(questionText: string): string {
  const envVar = process.env.DATAFORM_CLI_TEST_INPUTS;
  if (!envVar) {
    throw new Error("Environment variable DATAFORM_CLI_TEST_INPUTS not set.");
  }

  try {
    const parsed = JSON.parse(envVar);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      const inputs = new Map<string, string>(
        Object.entries(parsed).map(([key, value]) => [key, String(value)]),
      );
      const trimmedQuestion = questionText.trim();
      const answer = inputs.get(trimmedQuestion);

      if (answer !== undefined) {
        print(`[TEST_INPUT for "${trimmedQuestion}"]: "${answer}"`);
        return answer;
      } else {
        throw new Error(`[MISSING TEST_INPUT for "${trimmedQuestion}"]`);
      }
    } else {
      throw new Error(
        `Failed to parse DATAFORM_CLI_TEST_INPUTS: Expected a JSON object, but got ${typeof parsed}.`,
      );
    }
  } catch (e) {
    throw new Error(`Failed to parse DATAFORM_CLI_TEST_INPUTS: ${e.message || e}`);
  }
}

export function passwordQuestion(questionText: string) {
  return prompt(questionText, {
    hideEchoBack: true,
    mask: "",
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
      limit: (value) => {
        const intValue = parseInt(value, 10);
        return !isNaN(intValue);
      },
      limitMessage: errorOutput("Entered value must be an integer."),
      prompt: `[${defaultValue}] `,
      defaultInput: `${defaultValue}`,
    }),
    10,
  );
}

export function selectionQuestion(questionText: string, options: string[]) {
  return readlineSync.keyInSelect(options, `${questionText}\n`, {
    cancel: false,
  });
}

function prompt(questionText: string, options?: readlineSync.BasicOptions) {
  writeStdOut(questionText);
  return readlineSync.prompt({
    ...options,
    prompt: (options && options.prompt && options.prompt + DEFAULT_PROMPT) || DEFAULT_PROMPT,
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

export function printWarning(warningText: string, indentCount: number = 0) {
  writeStdErr(warningOutput(warningText), indentCount);
}

export function printInitResult(result: IInitResult) {
  if (result.dirsCreated && result.dirsCreated.length) {
    writeStdOut(successOutput("Directories successfully created:"));
    result.dirsCreated.forEach((dir) => writeStdOut(dir, 1));
  }
  if (result.filesWritten && result.filesWritten.length) {
    writeStdOut(successOutput("Files successfully written:"));
    result.filesWritten.forEach((file) => writeStdOut(file, 1));
  }
}

export function printInitCredsResult(writtenFilePath: string) {
  writeStdOut(successOutput("Credentials file successfully written:"));
  writeStdOut(writtenFilePath, 1);
  writeStdOut("To change connection settings, edit this file directly.");
}

export function isInteractive({ stream = process.stdout } = {}) {
  return Boolean(stream && stream.isTTY && process.env.TERM !== "dumb" && !("CI" in process.env));
}

export enum compiledGraphOutputType {
  Json = "json",
  Dot = "dot",
  Mermaid = "mermaid",
  Summary = "summary",
}

export function printCompiledGraph(
  graph: dataform.ICompiledGraph,
  outputType: compiledGraphOutputType,
  quietCompilation: boolean = false,
) {
  const interactive = isInteractive();

  if (outputType === compiledGraphOutputType.Json) {
    writeStdOut(prettyJsonStringify(graph));
  } else if (outputType === compiledGraphOutputType.Dot) {
    writeStdOut(dotRepresentation(graph, interactive));
  } else if (outputType === compiledGraphOutputType.Mermaid) {
    const mermaid = mermaidRepresentation(graph);
    writeStdOut(mermaid);
    const sizeWarning = mermaidSizeWarning(mermaid);
    if (sizeWarning) {
      printWarning(sizeWarning);
    }
  } else {
    const actionCount =
      0 +
      (graph.tables ? graph.tables.length : 0) +
      (graph.assertions ? graph.assertions.length : 0) +
      (graph.operations ? graph.operations.length : 0) +
      (graph.propertyGraphs ? graph.propertyGraphs.length : 0);
    writeStdOut(successOutput(`Compiled ${actionCount} action(s).`));
    if (graph.tables && graph.tables.length) {
      graph.tables.forEach(setOrValidateTableEnumType);
      writeStdOut(`${graph.tables.length} dataset(s)${quietCompilation ? "." : ":"}`);
      if (!quietCompilation) {
        graph.tables.forEach((compiledTable) => {
          writeStdOut(
            `${datasetString(
              compiledTable.target,
              tableTypeEnumToString(compiledTable.enumType),
              compiledTable.disabled,
            )}`,
            1,
          );
        });
      }
    }
    if (graph.assertions && graph.assertions.length) {
      writeStdOut(`${graph.assertions.length} assertion(s)${quietCompilation ? "." : ":"}`);
      if (!quietCompilation) {
        graph.assertions.forEach((assertion) => {
          writeStdOut(assertionString(assertion.target, assertion.disabled), 1);
        });
      }
    }
    if (graph.operations && graph.operations.length) {
      writeStdOut(`${graph.operations.length} operation(s)${quietCompilation ? "." : ":"}`);
      if (!quietCompilation) {
        graph.operations.forEach((operation) => {
          writeStdOut(operationString(operation.target, operation.disabled), 1);
        });
      }
    }
    if (graph.propertyGraphs && graph.propertyGraphs.length) {
      writeStdOut(
        `${graph.propertyGraphs.length} property graph(s)${quietCompilation ? "." : ":"}`,
      );
      if (!quietCompilation) {
        graph.propertyGraphs.forEach((propertyGraph) => {
          writeStdOut(propertyGraphString(propertyGraph.target, false), 1);
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

export function printCompiledGraphErrors(
  graphErrors: dataform.IGraphErrors,
  quietCompilation: boolean = false,
) {
  if (graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0) {
    printError("Compilation errors:", 1);
    graphErrors.compilationErrors.forEach((compileError) => {
      writeStdErr(
        `${calloutOutput(compileError.fileName)}: ${errorOutput(
          quietCompilation
            ? compileError.message + " " + formatStackTraceForQuietCompilation(compileError) ||
                compileError.stack
            : compileError.stack || compileError.message,
        )}`,
        1,
      );
    });
  }
}

export function printTestResult(testResult: dataform.ITestResult) {
  writeStdOut(
    `${testResult.name}: ${testResult.successful ? successOutput("passed") : errorOutput("failed")}`,
  );
  if (!testResult.successful) {
    testResult.messages.forEach((message) => writeStdErr(message, 1));
  }
}

export function printExecutionGraph(executionGraph: dataform.ExecutionGraph, asJson: boolean) {
  if (asJson) {
    writeStdOut(prettyJsonStringify(executionGraph.toJSON()));
  } else {
    const tableMetadataByTarget = new Map<string, dataform.ITableMetadata>();
    executionGraph.warehouseState?.tables?.forEach((tableMetadata) => {
      if (tableMetadata.target) {
        tableMetadataByTarget.set(targetStringifier.stringify(tableMetadata.target), tableMetadata);
      }
    });

    const actionsByType = {
      table: [] as dataform.IExecutionAction[],
      assertion: [] as dataform.IExecutionAction[],
      operation: [] as dataform.IExecutionAction[],
      propertyGraph: [] as dataform.IExecutionAction[],
    };
    executionGraph.actions.forEach((action) => {
      if (!(
        action.type === "table" ||
        action.type === "assertion" ||
        action.type === "operation" ||
        action.type === "propertyGraph"
      )) {
        throw new Error(`Unrecognized action type: ${action.type}`);
      }
      actionsByType[action.type].push(action);
    });
    const datasetActions = actionsByType.table;
    if (datasetActions && datasetActions.length) {
      writeStdOut(`${datasetActions.length} dataset(s):`);
      datasetActions.forEach((datasetAction) => {
        const tableMetadata = datasetAction.target
          ? tableMetadataByTarget.get(targetStringifier.stringify(datasetAction.target))
          : undefined;
        const datasetType = datasetAction.tableType || datasetAction.type;
        writeStdOut(
          `${datasetString(
            datasetAction.target,
            datasetType,
            datasetAction.tasks.length === 0,
          )} ${formatDatasetWarehouseState(datasetType, tableMetadata)}`,
          1,
        );
      });
    }
    const assertionActions = actionsByType.assertion;
    if (assertionActions && assertionActions.length) {
      writeStdOut(`${assertionActions.length} assertion(s):`);
      assertionActions.forEach((assertionAction) =>
        writeStdOut(assertionString(assertionAction.target, assertionAction.tasks.length === 0), 1),
      );
    }
    const operationActions = actionsByType.operation;
    if (operationActions && operationActions.length) {
      writeStdOut(`${operationActions.length} operation(s):`);
      operationActions.forEach((operationAction) =>
        writeStdOut(operationString(operationAction.target, operationAction.tasks.length === 0), 1),
      );
    }
    const propertyGraphActions = actionsByType.propertyGraph;
    if (propertyGraphActions && propertyGraphActions.length) {
      writeStdOut(`${propertyGraphActions.length} property graph(s):`);
      propertyGraphActions.forEach((propertyGraphAction) =>
        writeStdOut(
          propertyGraphString(propertyGraphAction.target, propertyGraphAction.tasks.length === 0),
          1,
        ),
      );
    }
  }
}

export function printExecutedAction(
  executedAction: dataform.IActionResult,
  executionAction: dataform.IExecutionAction,
  dryRun?: boolean,
) {
  const jobIds = executedAction.tasks
    .filter((task) => task.metadata?.bigquery?.jobId)
    .map((task) => task.metadata.bigquery.jobId);
  const bytesBilled = executedAction.tasks
    .filter((task) => task.metadata?.bigquery?.jobId)
    .map((task) => {
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
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${successOutput(
              `Assertion ${dryRun ? "dry run success" : "passed"}: `,
            )} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${successOutput(
              `Operation ${dryRun ? "dry run success" : "completed successfully"}: `,
            )} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          return;
        }
        case "propertyGraph": {
          writeStdOut(
            `${successOutput(
              `Property graph ${dryRun ? "dry run success" : "created"}: `,
            )} ${propertyGraphString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          return;
        }
      }
    }
    case dataform.ActionResult.ExecutionStatus.FAILED: {
      switch (executionAction.type) {
        case "table": {
          writeStdErr(
            `${errorOutput(`Dataset ${dryRun ? "dry run" : "creation"} failed: `)} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          break;
        }
        case "assertion": {
          writeStdErr(
            `${errorOutput(`Assertion ${dryRun ? "dry run " : ""}failed: `)} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          break;
        }
        case "operation": {
          writeStdErr(
            `${errorOutput(`Operation ${dryRun ? "dry run " : ""}failed: `)} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          break;
        }
        case "propertyGraph": {
          writeStdErr(
            `${errorOutput(
              `Property graph ${dryRun ? "dry run" : "creation"} failed: `,
            )} ${propertyGraphString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${executionSuffix}`,
          );
          break;
        }
      }
      printExecutedActionErrors(executedAction, executionAction);
      return;
    }
    case dataform.ActionResult.ExecutionStatus.SKIPPED: {
      const skipReason = executedAction.tasks?.[0]?.errorMessage;
      const skipSuffix = skipReason ? ` (${skipReason})` : "";
      switch (executionAction.type) {
        case "table": {
          writeStdOut(
            `${warningOutput("Skipping dataset creation: ")} ${datasetString(
              executionAction.target,
              executionAction.tableType,
              executionAction.tasks.length === 0,
            )}${skipSuffix}`,
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput("Skipping assertion execution: ")} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${skipSuffix}`,
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput("Skipping operation execution: ")} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}${skipSuffix}`,
          );
          return;
        }
        case "propertyGraph": {
          writeStdOut(
            `${warningOutput(
              dryRun
                ? "Property graph dry run skipped (upstream tables not materialised): "
                : "Skipping property graph creation: ",
            )} ${propertyGraphString(executionAction.target, executionAction.tasks.length === 0)}`,
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
              executionAction.tasks.length === 0,
            )}`,
          );
          return;
        }
        case "assertion": {
          writeStdOut(
            `${warningOutput(`Assertion execution disabled: `)} ${assertionString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}`,
          );
          return;
        }
        case "operation": {
          writeStdOut(
            `${warningOutput(`Operation execution disabled: `)} ${operationString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}`,
          );
          return;
        }
        case "propertyGraph": {
          writeStdOut(
            `${warningOutput(`Property graph creation disabled: `)} ${propertyGraphString(
              executionAction.target,
              executionAction.tasks.length === 0,
            )}`,
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
  }>,
) {
  const sorted = formatResults.sort((a, b) => a.filename.localeCompare(b.filename));
  const successfulFormatResults = sorted.filter((result) => !result.err && !result.needsFormatting);
  const needsFormattingResults = sorted.filter((result) => !result.err && result.needsFormatting);
  const failedFormatResults = sorted.filter((result) => !!result.err);

  if (successfulFormatResults.length > 0) {
    printSuccess("Successfully formatted:");
    successfulFormatResults.forEach((result) => writeStdOut(result.filename, 1));
  }

  if (needsFormattingResults.length > 0) {
    printError("Files that need formatting:");
    needsFormattingResults.forEach((result) => writeStdErr(result.filename, 1));
  }

  if (failedFormatResults.length > 0) {
    printError("Errors encountered during formatting:");
    failedFormatResults.forEach((result) =>
      writeStdOut(`${result.filename}: ${result.err.message}`, 1),
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

function propertyGraphString(target: dataform.ITarget, disabled: boolean) {
  return `${targetString(target)}${disabled ? " [disabled]" : ""}`;
}

function plainTargetString(target: dataform.ITarget) {
  return `${target.schema}.${target.name}`;
}

function targetString(target: dataform.ITarget) {
  return calloutOutput(`${target.schema}.${target.name}`);
}

export function dotRepresentation(graph: dataform.ICompiledGraph, interactive: boolean): string {
  const nodes: string[] = [];
  const edges: string[] = [];

  const formatTarget = interactive ? targetString : plainTargetString;

  graph.tables?.forEach((table) => {
    const nodeName = `${formatTarget(table.target)}`;
    nodes.push(
      `"${nodeName}" [label="${formatTarget(table.target)} [${tableTypeEnumToString(table.enumType)}]"]`,
    );
    table.dependencyTargets?.forEach((dependencyTarget) => {
      edges.push(`"${formatTarget(dependencyTarget)}" -> "${nodeName}"`);
    });
  });

  graph.assertions?.forEach((assertion) => {
    const nodeName = `${formatTarget(assertion.target)}`;
    nodes.push(`"${nodeName}" [label="${formatTarget(assertion.target)}"]`);
    assertion.dependencyTargets?.forEach((dependencyTarget) => {
      edges.push(`"${formatTarget(dependencyTarget)}" -> "${nodeName}"`);
    });
  });

  graph.operations?.forEach((operation) => {
    const nodeName = `${formatTarget(operation.target)}`;
    nodes.push(`"${nodeName}" [label="${formatTarget(operation.target)}"]`);
    operation.dependencyTargets?.forEach((dependencyTarget) => {
      edges.push(`"${formatTarget(dependencyTarget)}" -> "${nodeName}"`);
    });
  });

  return `digraph {\n${nodes.join(";\n")};\n${edges.join(";\n")};\n}`;
}

/**
 * Renders the compiled graph as a Mermaid flowchart, which GitHub, GitLab and other Mermaid-enabled
 * tools render natively. Shows the same action categories and dependency relationships as the DOT
 * output, but keeps same-named targets in different databases as separate nodes (and labels them
 * with their database, so they can be told apart).
 *
 * The output only depends on the graph itself, so it diffs cleanly when regenerated: nodes are
 * sorted by target, edges by the targets they connect, and node IDs are derived from the target
 * (see mermaidNodeIds).
 */
export function mermaidRepresentation(graph: dataform.ICompiledGraph): string {
  // Keyed by targetKey(): the target, and the table type for tables (shown in the label).
  const nodes = new Map<string, { target: dataform.ITarget; tableType?: string }>();
  const edges = new Map<string, { from: string; to: string }>();

  const addAction = (
    action: { target?: dataform.ITarget; dependencyTargets?: dataform.ITarget[] },
    tableType?: string,
  ) => {
    const to = targetKey(action.target);
    // An action's own entry replaces the plain one it got if it was first seen as a dependency.
    nodes.set(to, { target: action.target, tableType });
    action.dependencyTargets?.forEach((dependencyTarget) => {
      // Dependencies outside the printed graph (declarations, or actions pruned by --output-*)
      // still get a node, with a plain label.
      const from = targetKey(dependencyTarget);
      if (!nodes.has(from)) {
        nodes.set(from, { target: dependencyTarget });
      }
      edges.set(JSON.stringify([from, to]), { from, to });
    });
  };

  graph.tables?.forEach((table) => addAction(table, tableTypeEnumToString(table.enumType)));
  graph.assertions?.forEach((assertion) => addAction(assertion));
  graph.operations?.forEach((operation) => addAction(operation));

  // Labels show `schema.name`, plus the database when several nodes share the same `schema.name`.
  const schemaNameCounts = new Map<string, number>();
  nodes.forEach(({ target }) => {
    const schemaName = plainTargetString(target);
    schemaNameCounts.set(schemaName, (schemaNameCounts.get(schemaName) || 0) + 1);
  });
  const label = ({ target, tableType }: { target: dataform.ITarget; tableType?: string }) => {
    const schemaName = plainTargetString(target);
    const name =
      schemaNameCounts.get(schemaName) > 1 ? `${target.database}.${schemaName}` : schemaName;
    return tableType === undefined ? name : `${name} [${tableType}]`;
  };

  const keys = Array.from(nodes.keys()).sort(compareStrings);
  const ids = mermaidNodeIds(keys);
  const nodeLines = keys.map(
    (key) => `  ${ids.get(key)}["${escapeMermaidLabel(label(nodes.get(key)))}"]`,
  );
  const edgeLines = Array.from(edges.values())
    .sort((a, b) => compareStrings(a.from, b.from) || compareStrings(a.to, b.to))
    .map((edge) => `  ${ids.get(edge.from)} --> ${ids.get(edge.to)}`);

  // `graph` rather than the newer `flowchart` keyword: they draw the same diagram, but some
  // renderers (e.g. Azure DevOps wikis) only support `graph`.
  return ["graph LR", ...nodeLines, ...edgeLines].join("\n");
}

// Mermaid's default size limits. Renderers can configure them; GitHub refuses diagrams over
// 50,000 characters.
const MERMAID_MAX_EDGES = 500;
const MERMAID_MAX_TEXT_SIZE = 50000;

/**
 * Returns a warning if the diagram is larger than Mermaid renders by default, or undefined if it
 * fits. Renderers using the default limits show an error instead of such a diagram.
 */
export function mermaidSizeWarning(mermaid: string): string | undefined {
  // Labels escape `>`, so ` --> ` only appears in edge lines.
  const edgeCount = mermaid.split("\n").filter((line) => line.includes(" --> ")).length;
  if (edgeCount <= MERMAID_MAX_EDGES && mermaid.length <= MERMAID_MAX_TEXT_SIZE) {
    return undefined;
  }
  return (
    `Warning: this diagram has ${edgeCount} edges and ${mermaid.length} characters, which ` +
    `exceeds Mermaid's default limits (${MERMAID_MAX_EDGES} edges, ${MERMAID_MAX_TEXT_SIZE} ` +
    `characters), so many renderers (including GitHub) may refuse it. Use --output-actions or ` +
    `--output-tags to print part of the graph.`
  );
}

function targetKey(target: dataform.ITarget) {
  return JSON.stringify([target.database || "", target.schema || "", target.name || ""]);
}

/**
 * Assigns each target key a short Mermaid node ID: `n` followed by the start of the key's SHA-256
 * hash. IDs are short because renderers limit diagram size (GitHub refuses diagrams over 50,000
 * characters), and every edge repeats two IDs. They contain only letters and digits, so no escaping
 * is needed, and the `n` prefix keeps them clear of Mermaid keywords such as `end`.
 *
 * The prefix is 8 hex characters. Keys whose prefixes collide get longer IDs, just long enough to
 * differ from each other, so different targets never get the same ID and other IDs are unaffected.
 */
export function mermaidNodeIds(keys: string[]): Map<string, string> {
  const hashes = keys.map((key) => crypto.createHash("sha256").update(key).digest("hex"));
  const hashesByPrefix = new Map<string, string[]>();
  hashes.forEach((hash) => {
    const prefix = hash.slice(0, 8);
    hashesByPrefix.set(prefix, [...(hashesByPrefix.get(prefix) || []), hash]);
  });
  return new Map(
    keys.map((key, i) => {
      const hash = hashes[i];
      const others = hashesByPrefix.get(hash.slice(0, 8)).filter((other) => other !== hash);
      let length = 8;
      while (others.some((other) => other.startsWith(hash.slice(0, length)))) {
        length++;
      }
      return [key, `n${hash.slice(0, length)}`];
    }),
  );
}

function compareStrings(a: string, b: string) {
  return a < b ? -1 : a > b ? 1 : 0;
}

function escapeMermaidLabel(label: string) {
  return label.replace(/["#&<>`\\:$\u0000-\u001f\u007f\u0085\u2028\u2029]/g, (char) => {
    const code = char.codePointAt(0);
    // Control characters and line separators would break the label across lines, so show them as
    // Unicode control pictures instead (e.g. a line feed as ␊; ␤ for the other line separators).
    if (code < 0x20) {
      return String.fromCodePoint(0x2400 + code);
    }
    if (code === 0x7f) {
      return "\u2421";
    }
    if (code === 0x85 || code === 0x2028 || code === 0x2029) {
      return "\u2424";
    }
    // Mermaid reads `#<decimal code>;` as an escaped character. Besides `"` (which ends the label),
    // this escapes `#` (so a literal `#quot;` isn't decoded), `&`, `<` and `>` (so the label isn't
    // read as HTML, e.g. `<br>`), backticks (Markdown formatting), backslashes (`\n` is read as a
    // line break), colons (`fa:fa-user` is read as an icon) and dollar signs (`$$...$$` is read as
    // math).
    return `#${code};`;
  });
}

function printExecutedActionErrors(
  executedAction: dataform.IActionResult,
  executionAction: dataform.IExecutionAction,
) {
  const failingTasks = executedAction.tasks.filter(
    (task) => task.status === dataform.TaskResult.ExecutionStatus.FAILED,
  );
  failingTasks.forEach((task, i) => {
    // For JiT actions, the original executionAction.tasks might be empty
    // since they are generated during re-compilation.
    const statement = task.compiledSql || executionAction.tasks[i]?.statement;
    if (statement) {
      statement.split("\n").forEach((line: string) => {
        writeStdErr(`${DEFAULT_PROMPT}${line}`, 1);
      });
    }
    printError(task.errorMessage, 1);
  });
}
