import { ProjectConfigOptions } from "df/cli/project_config_options";
import { ICommandBase, INamedOption } from "df/cli/yargswrapper";

// Generates the parts of cli/README.md that are derived from the CLI command definitions.
//
// Generated content lives between marker comments, which don't render on GitHub or npm:
//
//   <!-- BEGIN GENERATED: options run -->
//   ...
//   <!-- END GENERATED: options run -->
//
// Everything outside the markers is hand-written and left untouched. Supported blocks:
//   commands                 the list of commands, linking to their sections
//   usage <command>          the command's usage line
//   positionals <command>    the command's positional arguments table
//   options <command>        the command's options table
//   project-config-options   the options shared by commands that override workflow_settings.yaml

// Command used to regenerate cli/README.md, shown in error messages.
export const UPDATE_README_COMMAND = "bazel run //cli:update_readme";

const BEGIN_MARKER = /^<!-- BEGIN GENERATED: (.+?) -->$/;
const END_MARKER = /^<!-- END GENERATED: (.+?) -->$/;

const PROJECT_CONFIG_OPTIONS_BLOCK = "project-config-options";
const COMMANDS_BLOCK = "commands";
const PER_COMMAND_BLOCK_KINDS = ["usage", "positionals", "options"];

type AnyOption = INamedOption<any, any>;

export interface IGeneratedReadme {
  // The README with all generated blocks regenerated.
  content: string;
  // Names of the blocks whose content changed, e.g. "options run".
  changedBlocks: string[];
  // Details of the blocks whose content changed, for error messages.
  staleBlocks: IStaleBlock[];
}

export interface IStaleBlock {
  name: string;
  // 1-based line number in the README of the first line inside the block.
  firstLine: number;
  // The block's lines as they are in the README.
  current: string[];
  // The block's lines as generated from the CLI definitions.
  expected: string[];
}

export function generateReadme(readme: string, commands: ICommandBase[]): IGeneratedReadme {
  const lines = readme.replace(/\r\n/g, "\n").split("\n");
  const output: string[] = [];
  const staleBlocks: IStaleBlock[] = [];
  const seenBlocks = new Set<string>();

  let openBlock: { name: string; line: number; content: string[] } | null = null;
  lines.forEach((line, index) => {
    const lineNumber = index + 1;
    const begin = line.match(BEGIN_MARKER);
    const end = line.match(END_MARKER);
    if (begin) {
      if (openBlock) {
        throw new Error(
          `cli/README.md:${lineNumber}: found "BEGIN GENERATED: ${begin[1]}" before ` +
            `"END GENERATED: ${openBlock.name}" (opened on line ${openBlock.line}).`,
        );
      }
      const name = begin[1];
      if (seenBlocks.has(name)) {
        throw new Error(`cli/README.md:${lineNumber}: duplicate generated block "${name}".`);
      }
      seenBlocks.add(name);
      openBlock = { name, line: lineNumber, content: [] };
      output.push(line);
    } else if (end) {
      if (!openBlock || end[1] !== openBlock.name) {
        throw new Error(
          `cli/README.md:${lineNumber}: "END GENERATED: ${end[1]}" doesn't match ` +
            (openBlock ? `"BEGIN GENERATED: ${openBlock.name}".` : "any BEGIN GENERATED marker."),
        );
      }
      const generated = renderBlock(openBlock.name, commands);
      if (openBlock.content.join("\n") !== generated.join("\n")) {
        staleBlocks.push({
          name: openBlock.name,
          firstLine: openBlock.line + 1,
          current: openBlock.content,
          expected: generated,
        });
      }
      output.push(...generated, line);
      openBlock = null;
    } else if (openBlock) {
      openBlock.content.push(line);
    } else {
      output.push(line);
    }
  });
  if (openBlock) {
    throw new Error(
      `cli/README.md:${openBlock.line}: "BEGIN GENERATED: ${openBlock.name}" is never closed.`,
    );
  }

  checkAllBlocksPresent(seenBlocks, commands);
  return {
    content: output.join("\n"),
    changedBlocks: staleBlocks.map((block) => block.name),
    staleBlocks,
  };
}

// Explains which generated blocks of cli/README.md are out of date, showing only the lines that
// differ, and how to fix it.
export function formatStaleBlocks(staleBlocks: IStaleBlock[]): string {
  const blocks = staleBlocks.map((block) => {
    const lastLine = block.firstLine + Math.max(block.current.length - 1, 0);
    const labelled = diffLines(block.current, block.expected).map(({ kind, line, index }) => ({
      label:
        kind === "current" ? `README (line ${block.firstLine + index}):` : "expected from cli/:",
      line,
    }));
    // Align the lines so that the differences are easy to spot.
    const labelWidth = Math.max(...labelled.map(({ label }) => label.length));
    const diff = labelled.map(({ label, line }) => `  ${label.padEnd(labelWidth)} ${line}`);
    return [
      `Block "${block.name}" (cli/README.md, lines ${block.firstLine}-${lastLine}):`,
      ...diff,
    ].join("\n");
  });
  return [
    "cli/README.md is out of date with the CLI command and option definitions in cli/.",
    "",
    ...blocks.flatMap((block) => [block, ""]),
    "To fix it:",
    `- If you changed a command or option in cli/, run: ${UPDATE_README_COMMAND}`,
    "- If you edited a generated block in cli/README.md by hand, revert that edit and change the",
    "  definition in cli/ instead (cli/commands/*.ts, cli/common_options.ts or",
    `  cli/project_config_options.ts), then run: ${UPDATE_README_COMMAND}`,
  ].join("\n");
}

interface IDiffLine {
  kind: "current" | "expected";
  line: string;
  // Index of the line in `current` or `expected`, depending on `kind`.
  index: number;
}

// Returns the lines that differ between `current` and `expected` (based on their longest common
// subsequence), in order. Generated blocks are small, so the quadratic algorithm is fine.
function diffLines(current: string[], expected: string[]): IDiffLine[] {
  const common: number[][] = Array.from({ length: current.length + 1 }, () =>
    new Array(expected.length + 1).fill(0),
  );
  for (let i = current.length - 1; i >= 0; i--) {
    for (let j = expected.length - 1; j >= 0; j--) {
      common[i][j] =
        current[i] === expected[j]
          ? common[i + 1][j + 1] + 1
          : Math.max(common[i + 1][j], common[i][j + 1]);
    }
  }
  const diff: IDiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < current.length || j < expected.length) {
    if (i < current.length && j < expected.length && current[i] === expected[j]) {
      i++;
      j++;
    } else if (
      j >= expected.length ||
      (i < current.length && common[i + 1][j] >= common[i][j + 1])
    ) {
      diff.push({ kind: "current", line: current[i], index: i });
      i++;
    } else {
      diff.push({ kind: "expected", line: expected[j], index: j });
      j++;
    }
  }
  return diff;
}

function checkAllBlocksPresent(seenBlocks: Set<string>, commands: ICommandBase[]) {
  for (const required of [COMMANDS_BLOCK, PROJECT_CONFIG_OPTIONS_BLOCK]) {
    if (!seenBlocks.has(required)) {
      throw new Error(`cli/README.md has no "${required}" generated block.`);
    }
  }
  for (const command of commands) {
    const name = commandName(command);
    const missing = PER_COMMAND_BLOCK_KINDS.filter((kind) => !seenBlocks.has(`${kind} ${name}`));
    if (missing.length > 0) {
      throw new Error(
        `cli/README.md is missing the generated block(s) ${missing
          .map((kind) => `"${kind} ${name}"`)
          .join(", ")} for the "${name}" command. A new command needs a section like:\n\n` +
          commandSectionSkeleton(name),
      );
    }
  }
}

function commandSectionSkeleton(name: string): string {
  const block = (kind: string) =>
    `<!-- BEGIN GENERATED: ${kind} ${name} -->\n<!-- END GENERATED: ${kind} ${name} -->`;
  return [
    `### \`${name}\``,
    "",
    block("usage"),
    "",
    `TODO: describe what \`${name}\` does.`,
    "",
    "**Positional arguments**",
    "",
    block("positionals"),
    "",
    "**Options**",
    "",
    block("options"),
    "",
    `Then run: ${UPDATE_README_COMMAND}`,
  ].join("\n");
}

function renderBlock(blockName: string, commands: ICommandBase[]): string[] {
  if (blockName === COMMANDS_BLOCK) {
    return commands.map((command) => {
      const name = commandName(command);
      return `- [\`${name}\`](#${name})`;
    });
  }
  if (blockName === PROJECT_CONFIG_OPTIONS_BLOCK) {
    return renderOptionsTable(ProjectConfigOptions.allYargsOptions);
  }
  const [kind, name, ...rest] = blockName.split(" ");
  const command = commands.find((c) => commandName(c) === name);
  if (!PER_COMMAND_BLOCK_KINDS.includes(kind) || !name || rest.length > 0) {
    throw new Error(`cli/README.md: unknown generated block "${blockName}".`);
  }
  if (!command) {
    throw new Error(
      `cli/README.md: generated block "${blockName}" refers to the unknown command "${name}".`,
    );
  }
  switch (kind) {
    case "usage":
      return ["```", `dataform ${command.format}`, "```"];
    case "positionals":
      return renderPositionalsTable(command.positionalOptions);
    default:
      return renderCommandOptionsTable(command.options);
  }
}

function renderPositionalsTable(positionals: AnyOption[]): string[] {
  if (positionals.length === 0) {
    return ["None."];
  }
  return [
    "| Argument | Required | Description |",
    "|---|---|---|",
    ...positionals.map((positional) =>
      tableRow([
        codeCell(positional.name),
        requiredCell(positional),
        textCell(describe(positional)),
      ]),
    ),
  ];
}

function renderCommandOptionsTable(options: AnyOption[]): string[] {
  // Commands that accept all project-config overrides list them in a single row pointing to the
  // shared table, rather than repeating them in every command.
  const projectConfigOptions: AnyOption[] = ProjectConfigOptions.allYargsOptions;
  const hasAllProjectConfigOptions = projectConfigOptions.every((option) =>
    options.includes(option),
  );
  if (!hasAllProjectConfigOptions) {
    return renderOptionsTable(options);
  }
  const commandOptions = options.filter((option) => !projectConfigOptions.includes(option));
  return renderOptionsTable(commandOptions, [
    tableRow([
      "Project-config overrides",
      "—",
      "—",
      "See [Shared project-config flags](#shared-project-config-flags).",
    ]),
  ]);
}

function renderOptionsTable(options: AnyOption[], extraRows: string[] = []): string[] {
  if (options.length === 0 && extraRows.length === 0) {
    return ["None."];
  }
  return [
    "| Flag | Type | Default | Description |",
    "|---|---|---|---|",
    ...options.map((option) =>
      tableRow([
        codeCell(`--${option.name}`),
        option.option.type ? textCell(option.option.type) : "—",
        defaultCell(option),
        textCell(describe(option)),
      ]),
    ),
    ...extraRows,
  ];
}

function requiredCell(positional: AnyOption): string {
  if (positional.docs?.required || positional.option.demandOption) {
    return "**Yes**";
  }
  const defaultValue = defaultCell(positional);
  return defaultValue === "—" ? "No" : `No (default ${defaultValue})`;
}

// Table cells are built from three kinds of content, each rendered differently:
// - plain text (e.g. an option's `describe`, shared with `dataform help`): escaped, see textCell(),
// - values rendered as inline code (option names, declared defaults): see codeCell(),
// - markdown written for the README (`docs.default`, fixed strings in this file): used as is.

function defaultCell(option: AnyOption): string {
  if (option.docs?.default !== undefined) {
    return markdownCell(option.docs.default, `docs.default of --${option.name}`);
  }
  const value = option.option.default;
  if (value === undefined || value === null) {
    return "—";
  }
  return codeCell(typeof value === "string" ? value : JSON.stringify(value));
}

function describe(option: AnyOption): string {
  return String(option.option.describe || "");
}

// URLs in plain text, which GitHub turns into links.
const URL_PATTERN = /(https?:\/\/[^\s|<>]+)/;

// Escapes plain text for a markdown table cell, so that it renders exactly as written:
// - characters that markdown or GFM tables would interpret are backslash-escaped (see
//   escapeMarkdown()), except in URLs, where a backslash would break GitHub's autolinking,
// - HTML is escaped (e.g. the "<job-prefix>" in "dataform-<job-prefix>-" would otherwise be
//   hidden as an unknown tag).
function textCell(text: string): string {
  return (
    text
      .replace(/\s*\n\s*/g, " ")
      .split(URL_PATTERN)
      // split() with a capturing group puts the URLs at odd indexes.
      .map((part, index) => (index % 2 === 1 ? part : escapeMarkdown(part)))
      .join("")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
  );
}

// Backslash-escapes characters that markdown or GFM tables would interpret: `|` ends the cell,
// `*`, `_` and `~` format text, `[` and `]` start links, `` ` `` starts code. Backslashes are
// escaped first, so that the escapes added for the other characters can't be altered. `_` inside a
// word (e.g. workflow_settings.yaml) can't start or end emphasis, so it's only escaped at word
// boundaries, which keeps the README source readable.
function escapeMarkdown(text: string): string {
  return text
    .replace(/\\/g, "\\\\")
    .replace(/[|*[\]~`]/g, (char) => `\\${char}`)
    .replace(/(?<![A-Za-z0-9])_|_(?![A-Za-z0-9])/g, "\\_");
}

// Renders a value as inline code. Backslashes are literal inside code spans, so values that would
// need escaping (backticks, `|`, line breaks) aren't supported rather than escaped.
function codeCell(value: string): string {
  if (/[`|\n]/.test(value)) {
    throw new Error(
      `cli/README.md: can't render ${JSON.stringify(value)} as inline code in a table cell. ` +
        "Set the option's docs.default to the markdown to show instead.",
    );
  }
  return `\`${value}\``;
}

// Markdown written for the README is used as is, but can't contain line breaks or unescaped `|`,
// which would break the table.
function markdownCell(markdown: string, source: string): string {
  if (/\n|(^|[^\\])\|/.test(markdown)) {
    throw new Error(
      `cli/README.md: ${source} can't contain line breaks or unescaped "|": ${JSON.stringify(markdown)}`,
    );
  }
  return markdown;
}

function tableRow(cells: string[]): string {
  return `| ${cells.join(" | ")} |`;
}

function commandName(command: ICommandBase): string {
  return command.format.split(" ")[0];
}
