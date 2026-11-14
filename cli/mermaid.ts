import * as crypto from "crypto";

import { tableTypeLabel } from "df/cli/util";
import { dataform } from "df/protos/ts";

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

  graph.tables?.forEach((table) => addAction(table, tableTypeLabel(table)));
  graph.assertions?.forEach((assertion) => addAction(assertion));
  graph.operations?.forEach((operation) => addAction(operation));

  // Labels show `schema.name`, plus the database when several nodes share the same `schema.name`.
  const schemaNameCounts = new Map<string, number>();
  nodes.forEach(({ target }) => {
    const schemaName = schemaAndName(target);
    schemaNameCounts.set(schemaName, (schemaNameCounts.get(schemaName) || 0) + 1);
  });
  const label = ({ target, tableType }: { target: dataform.ITarget; tableType?: string }) => {
    const schemaName = schemaAndName(target);
    const name =
      // A target without a database (e.g. one in a project with no defaultDatabase) keeps the
      // plain `schema.name`; the same `schema.name` in another database is the one labelled.
      schemaNameCounts.get(schemaName) > 1 && target.database
        ? `${target.database}.${schemaName}`
        : schemaName;
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

function schemaAndName(target: dataform.ITarget) {
  return `${target.schema}.${target.name}`;
}
