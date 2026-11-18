// Shared engine for regenerating HTML-comment-delimited generated blocks in Markdown files
// (`<!-- BEGIN GENERATED: <name> -->` ... `<!-- END GENERATED: <name> -->`) and rendering
// GitHub-Flavored Markdown (GFM) table cells safely.

const BEGIN_MARKER = /^<!-- BEGIN GENERATED: (.+?) -->$/;
const END_MARKER = /^<!-- END GENERATED: (.+?) -->$/;

// URLs in plain text, which GitHub turns into links.
const URL_PATTERN = /(https?:\/\/[^\s|<>]+)/;

export interface IGeneratedMarkdown {
  // The markdown document with all generated blocks regenerated.
  content: string;
  // Names of the blocks whose content changed.
  changedBlocks: string[];
  // Details of the blocks whose content changed, for error messages.
  staleBlocks: IStaleBlock[];
}

export interface IStaleBlock {
  name: string;
  // 1-based line number in the markdown file of the first line inside the block.
  firstLine: number;
  // The block's lines as they are in the markdown file.
  current: string[];
  // The block's lines as generated from code/protos.
  expected: string[];
}

export interface IGenerateMarkedMarkdownOptions {
  filePath: string;
  renderBlock: (blockName: string) => string[];
  validateSeenBlocks?: (seenBlocks: Set<string>) => void;
}

export interface IFormatStaleBlocksOptions {
  filePath: string;
  expectedLabel: string;
  summaryLine: string;
  fixInstructions: string[];
}

interface IDiffLine {
  kind: "current" | "expected";
  line: string;
  // Index of the line in `current` or `expected`, depending on `kind`.
  index: number;
}

export function generateMarkedMarkdown(
  markdown: string,
  options: IGenerateMarkedMarkdownOptions,
): IGeneratedMarkdown {
  const { filePath, renderBlock, validateSeenBlocks } = options;
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
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
          `${filePath}:${lineNumber}: found "BEGIN GENERATED: ${begin[1]}" before ` +
            `"END GENERATED: ${openBlock.name}" (opened on line ${openBlock.line}).`,
        );
      }
      const name = begin[1];
      if (seenBlocks.has(name)) {
        throw new Error(`${filePath}:${lineNumber}: duplicate generated block "${name}".`);
      }
      seenBlocks.add(name);
      openBlock = { name, line: lineNumber, content: [] };
      output.push(line);
    } else if (end) {
      if (!openBlock || end[1] !== openBlock.name) {
        throw new Error(
          `${filePath}:${lineNumber}: "END GENERATED: ${end[1]}" doesn't match ` +
            (openBlock ? `"BEGIN GENERATED: ${openBlock.name}".` : "any BEGIN GENERATED marker."),
        );
      }
      const generated = renderBlock(openBlock.name);
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
      `${filePath}:${openBlock.line}: "BEGIN GENERATED: ${openBlock.name}" is never closed.`,
    );
  }

  if (validateSeenBlocks) {
    validateSeenBlocks(seenBlocks);
  }
  return {
    content: output.join("\n"),
    changedBlocks: staleBlocks.map((block) => block.name),
    staleBlocks,
  };
}

export function formatStaleBlocks(
  staleBlocks: IStaleBlock[],
  options: IFormatStaleBlocksOptions,
): string {
  const { filePath, expectedLabel, summaryLine, fixInstructions } = options;
  const blocks = staleBlocks.map((block) => {
    const lastLine = block.firstLine + Math.max(block.current.length - 1, 0);
    const labelled = diffLines(block.current, block.expected).map(({ kind, line, index }) => ({
      label: kind === "current" ? `README (line ${block.firstLine + index}):` : expectedLabel,
      line,
    }));
    const labelWidth = Math.max(...labelled.map(({ label }) => label.length));
    const diff = labelled.map(({ label, line }) => `  ${label.padEnd(labelWidth)} ${line}`);
    return [
      `Block "${block.name}" (${filePath}, lines ${block.firstLine}-${lastLine}):`,
      ...diff,
    ].join("\n");
  });
  return [
    summaryLine,
    "",
    ...blocks.flatMap((block) => [block, ""]),
    "To fix it:",
    ...fixInstructions,
  ].join("\n");
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

// Escapes plain text for a markdown table cell, so that it renders exactly as written:
// - characters that markdown or GFM tables would interpret are backslash-escaped (see
//   escapeMarkdown()), except in URLs, where a backslash would break GitHub's autolinking,
// - HTML is escaped (e.g. "<job-prefix>" would otherwise be hidden as an unknown tag).
export function textCell(text: string): string {
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
export function escapeMarkdown(text: string): string {
  return text
    .replace(/\\/g, "\\\\")
    .replace(/[|*[\]~`]/g, (char) => `\\${char}`)
    .replace(/(?<![A-Za-z0-9])_|_(?![A-Za-z0-9])/g, "\\_");
}

// Renders a value as inline code. Backslashes are literal inside code spans, so values that would
// need escaping (backticks, `|`, line breaks) aren't supported rather than escaped.
export function codeCell(value: string, filePath: string, hint?: string): string {
  if (/[`|\n]/.test(value)) {
    const suffix = hint ? ` ${hint}` : "";
    throw new Error(
      `${filePath}: can't render ${JSON.stringify(value)} as inline code in a table cell.${suffix}`,
    );
  }
  return `\`${value}\``;
}

// Markdown written for a README cell is used as is, but can't contain line breaks or unescaped
// `|`, which would break the table.
export function markdownCell(markdown: string, filePath: string, source: string): string {
  if (/\n|(^|[^\\])\|/.test(markdown)) {
    throw new Error(
      `${filePath}: ${source} can't contain line breaks or unescaped "|": ${JSON.stringify(markdown)}`,
    );
  }
  return markdown;
}

export function tableRow(cells: string[]): string {
  return `| ${cells.join(" | ")} |`;
}
