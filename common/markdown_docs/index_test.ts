import { expect } from "chai";

import {
  codeCell,
  formatStaleBlocks,
  generateMarkedMarkdown,
  markdownCell,
  tableRow,
  textCell,
} from "df/common/markdown_docs";
import { suite, test } from "df/testing";

suite("common/markdown_docs", () => {
  test("replaces marked blocks and reports stale blocks", () => {
    const input = [
      "# Heading",
      "<!-- BEGIN GENERATED: alpha -->",
      "old line",
      "<!-- END GENERATED: alpha -->",
      "Prose stays untouched.",
    ].join("\n");

    const result = generateMarkedMarkdown(input, {
      filePath: "doc/README.md",
      renderBlock: (name) => [`generated ${name}`],
    });

    expect(result.changedBlocks).to.deep.equal(["alpha"]);
    expect(result.content).to.equal(
      [
        "# Heading",
        "<!-- BEGIN GENERATED: alpha -->",
        "generated alpha",
        "<!-- END GENERATED: alpha -->",
        "Prose stays untouched.",
      ].join("\n"),
    );

    const formatted = formatStaleBlocks(result.staleBlocks, {
      filePath: "doc/README.md",
      expectedLabel: "expected:",
      summaryLine: "doc/README.md is stale.",
      fixInstructions: ["- Run bazel run //doc:update"],
    });
    expect(formatted).to.include('Block "alpha" (doc/README.md, lines 3-3):');
    expect(formatted).to.include("old line");
    expect(formatted).to.include("generated alpha");
  });

  test("formats and validates table cells", () => {
    expect(
      tableRow([
        codeCell("name", "doc/README.md"),
        markdownCell("`string` \\| `number`", "doc/README.md", "type"),
        textCell("Uses <tag> & a|b in workflow_settings.yaml."),
      ]),
    ).to.equal(
      "| `name` | `string` \\| `number` | Uses &lt;tag&gt; &amp; a\\|b in workflow_settings.yaml. |",
    );
  });
});
