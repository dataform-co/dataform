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

  test("warns and lists existing protected incremental tables", () => {
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
        {
          target: { database: "db", schema: "schema", name: "regular_table" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_disabled" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_jit" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_new" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_was_view" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).to.have.length(1);
    const plainOutput = stderrOutput.join("").replace(/\x1b\[[0-9;]*m/g, "");
    expect(plainOutput).equals(
      [
        "Warning: --full-refresh and --override-protected are both set. " +
          "The following protected incremental tables will be dropped and recreated: ",
        "  db.schema.inc_protected",
        "  db.schema.inc_jit",
        "",
      ].join("\n"),
    );
  });

  test("does not warn when no protected incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_unprotected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_unprotected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_unprotected" },
          protected: false,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
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
    warnAboutOverrideProtectedFullRefresh(
      {
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
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_new" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_was_view" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when only disabled incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
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
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_disabled" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
  });
});
