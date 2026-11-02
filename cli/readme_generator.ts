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
}

export function generateReadme(readme: string, commands: ICommandBase[]): IGeneratedReadme {
  const lines = readme.replace(/\r\n/g, "\n").split("\n");
  const output: string[] = [];
  const changedBlocks: string[] = [];
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
        changedBlocks.push(openBlock.name);
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
  return { content: output.join("\n"), changedBlocks };
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
        `\`${positional.name}\``,
        requiredCell(positional),
        positional.option.describe || "",
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
        `\`--${option.name}\``,
        option.option.type || "—",
        defaultCell(option),
        option.option.describe || "",
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

function defaultCell(option: AnyOption): string {
  if (option.docs?.default !== undefined) {
    return option.docs.default;
  }
  const value = option.option.default;
  if (value === undefined || value === null) {
    return "—";
  }
  return `\`${typeof value === "string" ? value : JSON.stringify(value)}\``;
}

function tableRow(cells: string[]): string {
  return `| ${cells.map(escapeTableCell).join(" | ")} |`;
}

function escapeTableCell(text: string): string {
  return String(text)
    .replace(/\s*\n\s*/g, " ")
    .replace(/\|/g, "\\|");
}

function commandName(command: ICommandBase): string {
  return command.format.split(" ")[0];
}
