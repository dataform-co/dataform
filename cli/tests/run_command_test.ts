import { expect } from "chai";

import { warnAboutOverrideProtectedFullRefresh } from "df/cli/commands/run_command";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("warnAboutOverrideProtectedFullRefresh", ({ beforeEach, afterEach }) => {
  const originalStderrWrite = process.stderr.write;
  let stderrOutput: string[];

  beforeEach("capture stderr", () => {
    stderrOutput = [];
    process.stderr.write = ((chunk: string | Uint8Array) => {
      stderrOutput.push(String(chunk));
      return true;
    }) as typeof process.stderr.write;
  });

  afterEach("restore stderr", () => {
    process.stderr.write = originalStderrWrite;
  });

  test("warns and lists existing incremental tables, marking protected ones", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_unprotected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "table",
            target: { database: "db", schema: "schema", name: "regular_table" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_protected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_disabled" },
            tasks: [],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_jit" },
            tasks: [],
            jitCode: "return 1;",
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_new" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_was_view" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_unprotected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "regular_table" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_protected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_disabled" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_jit" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_was_view" },
              type: dataform.TableMetadata.Type.VIEW,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_protected" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_unprotected" },
          protected: false,
        },
      ],
    );

    expect(stderrOutput).to.have.length(1);
    expect(stderrOutput[0]).to.include(
      "Warning: --full-refresh and --override-protected are both set. " +
        "This will purge data from all incremental tables, even protected ones.",
    );
    expect(stderrOutput[0]).to.include("  db.schema.inc_unprotected\n");
    expect(stderrOutput[0]).to.include("  db.schema.inc_protected (protected)\n");
    expect(stderrOutput[0]).to.include("  db.schema.inc_jit\n");
    expect(stderrOutput[0]).to.not.include("regular_table");
    expect(stderrOutput[0]).to.not.include("inc_disabled");
    expect(stderrOutput[0]).to.not.include("inc_new");
    expect(stderrOutput[0]).to.not.include("inc_was_view");
  });

  test("does not warn when no incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh({
      actions: [
        {
          type: "table",
          tableType: "table",
          target: { database: "db", schema: "schema", name: "regular_table" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "view",
          target: { database: "db", schema: "schema", name: "some_view" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "assertion",
          target: { database: "db", schema: "schema", name: "some_assertion" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "db", schema: "schema", name: "regular_table" },
            type: dataform.TableMetadata.Type.TABLE,
          },
        ],
      },
    });

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when incremental tables do not exist in warehouse yet", () => {
    warnAboutOverrideProtectedFullRefresh({
      actions: [
        {
          type: "table",
          tableType: "incremental",
          target: { database: "db", schema: "schema", name: "inc_new" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "incremental",
          target: { database: "db", schema: "schema", name: "inc_was_view" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "db", schema: "schema", name: "inc_was_view" },
            type: dataform.TableMetadata.Type.VIEW,
          },
        ],
      },
    });

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when only disabled incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh({
      actions: [
        {
          type: "table",
          tableType: "incremental",
          target: { database: "db", schema: "schema", name: "inc_disabled" },
          tasks: [],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "db", schema: "schema", name: "inc_disabled" },
            type: dataform.TableMetadata.Type.TABLE,
          },
        ],
      },
    });

    expect(stderrOutput).deep.equals([]);
  });
});
