import { ProjectConfigOptions } from "df/cli/project_config_options";
import { ICommandBase, INamedOption } from "df/cli/yargswrapper";
import {
  codeCell as sharedCodeCell,
  formatStaleBlocks as sharedFormatStaleBlocks,
  generateMarkedMarkdown,
  IGeneratedMarkdown,
  IStaleBlock,
  markdownCell as sharedMarkdownCell,
  tableRow,
  textCell,
} from "df/common/markdown_docs";

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

const README_PATH = "cli/README.md";

// Command used to regenerate cli/README.md, shown in error messages.
export const UPDATE_README_COMMAND = "bazel run //cli:update_readme";

const PROJECT_CONFIG_OPTIONS_BLOCK = "project-config-options";
const COMMANDS_BLOCK = "commands";
const PER_COMMAND_BLOCK_KINDS = ["usage", "positionals", "options"];

type AnyOption = INamedOption<any, any>;

export type IGeneratedReadme = IGeneratedMarkdown;
export { IStaleBlock };

export function generateReadme(readme: string, commands: ICommandBase[]): IGeneratedReadme {
  return generateMarkedMarkdown(readme, {
    filePath: README_PATH,
    renderBlock: (blockName) => renderBlock(blockName, commands),
    validateSeenBlocks: (seenBlocks) => checkAllBlocksPresent(seenBlocks, commands),
  });
}

// Explains which generated blocks of cli/README.md are out of date, showing only the lines that
// differ, and how to fix it.
export function formatStaleBlocks(staleBlocks: IStaleBlock[]): string {
  return sharedFormatStaleBlocks(staleBlocks, {
    filePath: README_PATH,
    expectedLabel: "expected from cli/:",
    summaryLine:
      "cli/README.md is out of date with the CLI command and option definitions in cli/.",
    fixInstructions: [
      `- If you changed a command or option in cli/, run: ${UPDATE_README_COMMAND}`,
      "- If you edited a generated block in cli/README.md by hand, revert that edit and change the",
      "  definition in cli/ instead (cli/commands/*.ts, cli/common_options.ts or",
      `  cli/project_config_options.ts), then run: ${UPDATE_README_COMMAND}`,
    ],
  });
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
  // shared table, rather than repeating them in every command. This relies on reference identity:
  // the command must reuse the ProjectConfigOptions.allYargsOptions objects, so a command that
  // wraps or copies one of them falls back to listing all its options in the full table.
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
    return sharedMarkdownCell(option.docs.default, README_PATH, `docs.default of --${option.name}`);
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

function codeCell(value: string): string {
  return sharedCodeCell(
    value,
    README_PATH,
    "Set the option's docs.default to the markdown to show instead.",
  );
}

function commandName(command: ICommandBase): string {
  return command.format.split(" ")[0];
}
