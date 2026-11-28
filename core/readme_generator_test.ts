import { expect } from "chai";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { suite, test } from "df/testing";
import {
  CORE_README_BLOCKS,
  formatStaleCoreBlocks,
  generateCoreReadme,
  ICoreDocData,
} from "df/tools/docs/core_readme_generator";
import { extractFirstProtoParagraph, loadDataformProtos } from "df/tools/docs/proto_extractor";
import { extractCoreExports } from "df/tools/docs/ts_extractor";

function block(name: string, ...content: string[]): string {
  return [`<!-- BEGIN GENERATED: ${name} -->`, ...content, `<!-- END GENERATED: ${name} -->`].join(
    "\n",
  );
}

function minimalReadme(extra = ""): string {
  return ["# Core", ...CORE_README_BLOCKS.map((name) => block(name)), extra].join("\n");
}

function generatedBlock(content: string, name: string): string {
  const begin = `<!-- BEGIN GENERATED: ${name} -->\n`;
  const start = content.indexOf(begin) + begin.length;
  return content.slice(start, content.indexOf(`<!-- END GENERATED: ${name} -->`, start)).trimEnd();
}

function sampleData(): ICoreDocData {
  return {
    workflowSettingsFields: [
      {
        name: "defaultDataset",
        typeMarkdown: "`string`",
        defaultMarkdown: "—",
        descriptionMarkdown:
          "Required. Default BigQuery dataset (`schema`) for tables, views, and operations.",
      },
      {
        name: "lineage",
        typeMarkdown: "`object`",
        defaultMarkdown: "—",
        descriptionMarkdown:
          "Knowledge Catalog Data Lineage settings (`lineage: { enabled: true }`).",
      },
    ],
    sqlxContextFunctions: [
      {
        name: "ref",
        signatureMarkdown: "`ref(name)` / `ref(schema, name)`",
        descriptionMarkdown: "Resolves an action target and adds a dependency.",
      },
    ],
    sessionGlobals: [
      {
        name: "publish",
        signatureMarkdown: "`publish(name, queryOrConfig?)`",
        descriptionMarkdown: "Creates a `table`, `view`, or `incremental` action.",
      },
      {
        name: "dataform.projectConfig",
        signatureMarkdown: "`dataform.ProjectConfig`",
        descriptionMarkdown: "Read-only access to the resolved project configuration.",
      },
    ],
    coreExports: [
      {
        name: "version",
        typeLabel: "string",
        descriptionMarkdown: "The installed `@dataform/core` semantic version string.",
      },
      {
        name: "main",
        typeLabel: "(coreExecutionRequest: Uint8Array | string) => Uint8Array | string",
        descriptionMarkdown: "Entry point invoked by the compilation VM sandbox.",
      },
    ],
  };
}

suite("core readme generator", () => {
  test("renders all four generated blocks from structured doc data", () => {
    const { content } = generateCoreReadme(minimalReadme("Hand-written prose."), sampleData());

    expect(generatedBlock(content, "workflow-settings")).to.equal(
      [
        "| Field | Type | Default | Description |",
        "|---|---|---|---|",
        "| `defaultDataset` | `string` | — | Required. Default BigQuery dataset (`schema`) for tables, views, and operations. |",
        "| `lineage` | `object` | — | Knowledge Catalog Data Lineage settings (`lineage: { enabled: true }`). |",
      ].join("\n"),
    );

    expect(generatedBlock(content, "sqlx-context-functions")).to.equal(
      [
        "| Function | Signature | Description |",
        "|---|---|---|",
        "| `ref` | `ref(name)` / `ref(schema, name)` | Resolves an action target and adds a dependency. |",
      ].join("\n"),
    );

    expect(generatedBlock(content, "session-globals")).to.equal(
      [
        "| Global Function / Object | Signature | Description |",
        "|---|---|---|",
        "| `publish` | `publish(name, queryOrConfig?)` | Creates a `table`, `view`, or `incremental` action. |",
        "| `dataform.projectConfig` | `dataform.ProjectConfig` | Read-only access to the resolved project configuration. |",
      ].join("\n"),
    );

    expect(generatedBlock(content, "core-exports")).to.equal(
      [
        "- **`version`** (`string`): The installed `@dataform/core` semantic version string.",
        "- **`main`** (`(coreExecutionRequest: Uint8Array | string) => Uint8Array | string`): Entry point invoked by the compilation VM sandbox.",
      ].join("\n"),
    );
    expect(content).to.include("Hand-written prose.");
  });

  test("is idempotent", () => {
    const data = sampleData();
    const first = generateCoreReadme(minimalReadme(), data);
    const second = generateCoreReadme(first.content, data);
    expect(second.content).to.equal(first.content);
    expect(second.changedBlocks).to.deep.equal([]);
  });

  test("formats actionable diff output when a block is stale", () => {
    const data = sampleData();
    const upToDate = generateCoreReadme(minimalReadme(), data).content;
    const stale = upToDate.replace(
      "Required. Default BigQuery dataset (`schema`) for tables, views, and operations.",
      "Outdated description.",
    );
    const { staleBlocks } = generateCoreReadme(stale, data);

    const formatted = formatStaleCoreBlocks(staleBlocks);
    expect(formatted).to.include(
      "core/README.md is out of date with the TypeScript and Protocol Buffer definitions in core/ and protos/.",
    );
    expect(formatted).to.include("Outdated description.");
    expect(formatted).to.include(
      "Required. Default BigQuery dataset (`schema`) for tables, views, and operations.",
    );
    expect(formatted).to.include("bazel run //core:update_readme");
  });

  test("fails fast on unescaped pipes or newlines in table cell markdown", () => {
    const badData = sampleData();
    badData.workflowSettingsFields[0].descriptionMarkdown = "Contains unescaped | pipe.";
    expect(() => generateCoreReadme(minimalReadme(), badData)).to.throw(
      /description of defaultDataset can't contain line breaks or unescaped "\|"/,
    );
  });

  test("fails when a required block is missing or unknown", () => {
    const data = sampleData();
    expect(() =>
      generateCoreReadme(minimalReadme().replace(block("core-exports"), ""), data),
    ).to.throw(/has no "core-exports" generated block/);

    expect(() => generateCoreReadme(minimalReadme(block("unknown-block")), data)).to.throw(
      /unknown generated block "unknown-block"/,
    );
  });

  test("extractFirstProtoParagraph keeps Required. and splits on blank lines", () => {
    expect(
      extractFirstProtoParagraph(
        "Required. Default dataset (e.g. `analytics`, v1.0).\n\nSecond paragraph details.",
      ),
    ).to.equal("Required. Default dataset (e.g. `analytics`, v1.0).");

    expect(
      extractFirstProtoParagraph("Optional. Custom variables.\nwrapped on second line."),
    ).to.equal("Custom variables. wrapped on second line.");
  });

  test("extractCoreExports enforces package entrypoint parity and distinguishes types", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "core-exports-test-"));
    try {
      const coreIdx = path.join(tmpDir, "core_index.ts");
      const pkgIdx = path.join(tmpDir, "pkg_index.ts");
      fs.writeFileSync(
        coreIdx,
        [
          'import { IFoo as Foo } from "./foo";',
          "/** Version string. */",
          'export const version: string = "1.0.0";',
          "/** Foo interface. */",
          "export type IFoo = Foo;",
          "/** Mode union. */",
          'export type Mode = "a" | "b";',
          "/** @internal */",
          "export function internalHelper(): void {}",
        ].join("\n"),
      );
      fs.writeFileSync(pkgIdx, 'export { IFoo, internalHelper, Mode, version } from "df/core";\n');

      expect(extractCoreExports(coreIdx, pkgIdx)).to.deep.equal([
        { name: "version", typeLabel: "string", descriptionMarkdown: "Version string." },
        { name: "IFoo", typeLabel: "interface", descriptionMarkdown: "Foo interface." },
        { name: "Mode", typeLabel: "type", descriptionMarkdown: "Mode union." },
      ]);

      fs.writeFileSync(pkgIdx, 'export { version } from "df/core";\n');
      expect(() => extractCoreExports(coreIdx, pkgIdx)).to.throw(/Export mismatch between/);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  test("loadDataformProtos fails fast when an entry proto references an unimported message", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "proto-standalone-test-"));
    try {
      fs.writeFileSync(
        path.join(tmpDir, "core.proto"),
        'syntax = "proto3";\npackage dataform;\nmessage OnlyInCore { string name = 1; }\n',
      );
      fs.writeFileSync(
        path.join(tmpDir, "configs.proto"),
        'syntax = "proto3";\npackage dataform;\nmessage WorkflowSettings { OnlyInCore x = 1; }\n',
      );

      expect(() => loadDataformProtos(tmpDir, ["configs.proto"])).to.throw(Error, /OnlyInCore/);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
