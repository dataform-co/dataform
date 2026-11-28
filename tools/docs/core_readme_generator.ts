import * as path from "path";

import {
  codeCell as sharedCodeCell,
  formatStaleBlocks as sharedFormatStaleBlocks,
  generateMarkedMarkdown,
  IGeneratedMarkdown,
  IStaleBlock,
  markdownCell as sharedMarkdownCell,
  tableRow,
} from "df/common/markdown_docs";
import {
  extractWorkflowSettingsFields,
  IWorkflowSettingsFieldDoc,
  loadDataformProtos,
} from "df/tools/docs/proto_extractor";
import {
  extractCoreExports,
  extractSessionGlobals,
  extractSqlxContextFunctions,
  IExportDoc,
  IFunctionDoc,
} from "df/tools/docs/ts_extractor";

const README_PATH = "core/README.md";

// Command used to regenerate core/README.md, shown in error messages.
export const UPDATE_README_COMMAND = "bazel run //core:update_readme";

export const CORE_README_BLOCKS = [
  "workflow-settings",
  "sqlx-context-functions",
  "session-globals",
  "core-exports",
] as const;

export interface ICoreDocData {
  workflowSettingsFields: IWorkflowSettingsFieldDoc[];
  sqlxContextFunctions: IFunctionDoc[];
  sessionGlobals: IFunctionDoc[];
  coreExports: IExportDoc[];
}

export interface ICoreSourcePaths {
  protosDir: string;
  contextablesPath: string;
  sessionPath: string;
  coreIndexPath: string;
  packageIndexPath: string;
}

export { IGeneratedMarkdown, IStaleBlock };

export function defaultCoreSourcePaths(workspaceRoot = "."): ICoreSourcePaths {
  return {
    protosDir: path.join(workspaceRoot, "protos"),
    contextablesPath: path.join(workspaceRoot, "core", "contextables.ts"),
    sessionPath: path.join(workspaceRoot, "core", "session.ts"),
    coreIndexPath: path.join(workspaceRoot, "core", "index.ts"),
    packageIndexPath: path.join(workspaceRoot, "packages", "@dataform", "core", "index.ts"),
  };
}

export function loadCoreDocData(paths: ICoreSourcePaths = defaultCoreSourcePaths()): ICoreDocData {
  const protoRoot = loadDataformProtos(paths.protosDir, ["configs.proto"]);
  return {
    workflowSettingsFields: extractWorkflowSettingsFields(protoRoot),
    sqlxContextFunctions: extractSqlxContextFunctions(paths.contextablesPath),
    sessionGlobals: extractSessionGlobals(paths.sessionPath),
    coreExports: extractCoreExports(paths.coreIndexPath, paths.packageIndexPath),
  };
}

export function generateCoreReadme(
  readme: string,
  data: ICoreDocData = loadCoreDocData(),
): IGeneratedMarkdown {
  return generateMarkedMarkdown(readme, {
    filePath: README_PATH,
    renderBlock: (blockName) => renderBlock(blockName, data),
    validateSeenBlocks: checkAllBlocksPresent,
  });
}

export function formatStaleCoreBlocks(staleBlocks: IStaleBlock[]): string {
  return sharedFormatStaleBlocks(staleBlocks, {
    filePath: README_PATH,
    currentLabel: "README",
    expectedLabel: "expected from core/ & protos/:",
    summaryLine:
      "core/README.md is out of date with the TypeScript and Protocol Buffer definitions in core/ and protos/.",
    fixInstructions: [
      `- If you changed public APIs or comments in core/ or protos/, run: ${UPDATE_README_COMMAND}`,
      "- If you edited a generated block in core/README.md by hand, revert that edit and update the",
      "  source definition or JSDoc/proto comment instead (core/contextables.ts, core/session.ts,",
      `  core/index.ts or protos/*.proto), then run: ${UPDATE_README_COMMAND}`,
    ],
  });
}

function checkAllBlocksPresent(seenBlocks: Set<string>) {
  for (const required of CORE_README_BLOCKS) {
    if (!seenBlocks.has(required)) {
      throw new Error(`${README_PATH} has no "${required}" generated block.`);
    }
  }
}

function renderBlock(blockName: string, data: ICoreDocData): string[] {
  switch (blockName) {
    case "workflow-settings":
      return renderWorkflowSettingsTable(data.workflowSettingsFields);
    case "sqlx-context-functions":
      return renderSqlxContextTable(data.sqlxContextFunctions);
    case "session-globals":
      return renderSessionGlobalsTable(data.sessionGlobals);
    case "core-exports":
      return renderCoreExportsList(data.coreExports);
    default:
      throw new Error(`${README_PATH}: unknown generated block "${blockName}".`);
  }
}

function renderWorkflowSettingsTable(fields: IWorkflowSettingsFieldDoc[]): string[] {
  return [
    "| Field | Type | Default | Description |",
    "|---|---|---|---|",
    ...fields.map((field) =>
      tableRow([
        codeCell(field.name),
        markdownCell(field.typeMarkdown, `type of ${field.name}`),
        markdownCell(field.defaultMarkdown, `default of ${field.name}`),
        markdownCell(field.descriptionMarkdown, `description of ${field.name}`),
      ]),
    ),
  ];
}

function renderSqlxContextTable(functions: IFunctionDoc[]): string[] {
  return [
    "| Function | Signature | Description |",
    "|---|---|---|",
    ...functions.map((fn) =>
      tableRow([
        codeCell(fn.name),
        markdownCell(fn.signatureMarkdown, `signature of ${fn.name}`),
        markdownCell(fn.descriptionMarkdown, `JSDoc of ${fn.name}`),
      ]),
    ),
  ];
}

function renderSessionGlobalsTable(globals: IFunctionDoc[]): string[] {
  return [
    "| Global Function / Object | Signature | Description |",
    "|---|---|---|",
    ...globals.map((item) =>
      tableRow([
        codeCell(item.name),
        markdownCell(item.signatureMarkdown, `signature of ${item.name}`),
        markdownCell(item.descriptionMarkdown, `JSDoc of ${item.name}`),
      ]),
    ),
  ];
}

function renderCoreExportsList(exportsList: IExportDoc[]): string[] {
  return exportsList.map(
    (item) => `- **\`${item.name}\`** (\`${item.typeLabel}\`): ${item.descriptionMarkdown}`,
  );
}

function codeCell(value: string): string {
  return sharedCodeCell(value, README_PATH);
}

function markdownCell(markdown: string, source: string): string {
  return sharedMarkdownCell(markdown, README_PATH, source);
}
