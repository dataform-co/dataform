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
  test("replaces marked blocks and reports stale blocks using filePath label", () => {
    const input = [
      "# Heading",
      "<!-- BEGIN GENERATED: alpha -->",
      "old line",
      "<!-- END GENERATED: alpha -->",
      "Prose stays untouched.",
    ].join("\n");

    const result = generateMarkedMarkdown(input, {
      filePath: "doc/guide.md",
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
      filePath: "doc/guide.md",
      expectedLabel: "expected:",
      summaryLine: "doc/guide.md is stale.",
      fixInstructions: ["- Run bazel run //doc:update"],
    });
    expect(formatted).to.include('Block "alpha" (doc/guide.md, lines 3-3):');
    expect(formatted).to.include("doc/guide.md (line 3): old line");
    expect(formatted).to.include("expected:              generated alpha");
  });

  test("rejects malformed generated markers", () => {
    const run = (doc: string) =>
      generateMarkedMarkdown(doc, { filePath: "doc/guide.md", renderBlock: () => [] });

    expect(() => run("<!-- BEGIN GENERATED: alpha -->\nunclosed")).to.throw(
      'doc/guide.md:1: "BEGIN GENERATED: alpha" is never closed.',
    );
    expect(() =>
      run(
        [
          "<!-- BEGIN GENERATED: alpha -->",
          "<!-- BEGIN GENERATED: beta -->",
          "<!-- END GENERATED: beta -->",
        ].join("\n"),
      ),
    ).to.throw(
      'doc/guide.md:2: found "BEGIN GENERATED: beta" before "END GENERATED: alpha" (opened on line 1).',
    );
    expect(() =>
      run(
        [
          "<!-- BEGIN GENERATED: alpha -->",
          "<!-- END GENERATED: alpha -->",
          "<!-- BEGIN GENERATED: alpha -->",
          "<!-- END GENERATED: alpha -->",
        ].join("\n"),
      ),
    ).to.throw('doc/guide.md:3: duplicate generated block "alpha".');
    expect(() =>
      run(["<!-- BEGIN GENERATED: alpha -->", "<!-- END GENERATED: beta -->"].join("\n")),
    ).to.throw('doc/guide.md:2: "END GENERATED: beta" doesn\'t match "BEGIN GENERATED: alpha".');
    expect(() => run("<!-- END GENERATED: alpha -->")).to.throw(
      'doc/guide.md:1: "END GENERATED: alpha" doesn\'t match any BEGIN GENERATED marker.',
    );
  });

  test("formats and validates table cells", () => {
    expect(
      tableRow([
        codeCell("name", "doc/guide.md"),
        markdownCell("`string` \\| `number`", "doc/guide.md", "type"),
        textCell("Uses <tag> & a|b in workflow_settings.yaml (https://example.com/a_b)."),
      ]),
    ).to.equal(
      "| `name` | `string` \\| `number` | Uses &lt;tag&gt; &amp; a\\|b in workflow_settings.yaml (https://example.com/a_b). |",
    );

    expect(() => codeCell("bad`tick", "doc/guide.md")).to.throw(/can't render "bad`tick"/);
    expect(() => codeCell("bad|pipe", "doc/guide.md")).to.throw(/can't render "bad\|pipe"/);
    expect(() => codeCell("bad\nline", "doc/guide.md")).to.throw(/can't render "bad\\nline"/);

    expect(() => markdownCell("bad | pipe", "doc/guide.md", "description")).to.throw(
      /description can't contain line breaks or unescaped "\|"/,
    );
    expect(() => markdownCell("line1\nline2", "doc/guide.md", "description")).to.throw(
      /description can't contain line breaks or unescaped "\|"/,
    );
  });
});
