import { expect } from "chai";
import Long from "long";

import { dotRepresentation, printExecutionGraph } from "df/cli/console";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("printExecutionGraph", () => {
  const timestampMillis = Long.fromNumber(Date.UTC(2026, 10, 4, 14, 8, 23));

  test("printExecutionGraph outputs plain text summary with warehouse state", () => {
    const executionGraph = dataform.ExecutionGraph.create({
      actions: [
        {
          type: "table",
          tableType: "table",
          target: { database: "proj", schema: "ds", name: "existing_table" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "view",
          target: { database: "proj", schema: "ds", name: "type_changed_to_view" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "incremental",
          target: { database: "proj", schema: "ds", name: "missing_table" },
          tasks: [],
        },
        {
          type: "assertion",
          target: { database: "proj", schema: "ds", name: "my_assertion" },
          tasks: [{ type: "assertion", statement: "SELECT 1" }],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "proj", schema: "ds", name: "existing_table" },
            type: dataform.TableMetadata.Type.TABLE,
            lastUpdatedMillis: timestampMillis,
          },
          {
            target: { database: "proj", schema: "ds", name: "type_changed_to_view" },
            type: dataform.TableMetadata.Type.TABLE,
            lastUpdatedMillis: timestampMillis,
          },
        ],
      },
    });

    const chunks: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = (chunk: string) => {
      chunks.push(String(chunk));
      return true;
    };
    try {
      printExecutionGraph(executionGraph, false);
    } finally {
      process.stdout.write = originalWrite;
    }

    const plainOutput = chunks.join("").replace(/\x1b\[[0-9;]*m/g, "");
    expect(plainOutput).equals(
      [
        "3 dataset(s):",
        "  ds.existing_table [table] (last modified: 2026-11-04 14:08:23 UTC)",
        "  ds.type_changed_to_view [view] (type: table -> view, last modified: 2026-11-04 14:08:23 UTC)",
        "  ds.missing_table [incremental] [disabled] (does not exist)",
        "1 assertion(s):",
        "  ds.my_assertion",
        "",
      ].join("\n"),
    );
  });
});

suite("dotRepresentation", () => {
  test("labels tables in graphs that only set the string type", () => {
    const tables = [
      { target: { database: "p", schema: "ds", name: "inc" }, type: "incremental" },
      {
        target: { database: "p", schema: "ds", name: "t" },
        type: "table",
        dependencyTargets: [{ database: "p", schema: "ds", name: "inc" }],
      },
    ];
    const expected = [
      "digraph {",
      '"ds.inc" [label="ds.inc [incremental]"];',
      '"ds.t" [label="ds.t [table]"];',
      '"ds.inc" -> "ds.t";',
      "}",
    ].join("\n");

    // As created in memory: enumType is unset.
    const created = dataform.CompiledGraph.create({ tables });
    expect(dotRepresentation(created, false)).equals(expected);
    // After a protobuf round-trip: the absent enumType defaults to UNKNOWN_TYPE.
    const decoded = dataform.CompiledGraph.decode(dataform.CompiledGraph.encode(created).finish());
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
    expect(dotRepresentation(decoded, false)).equals(expected);
    // The graph passed in isn't modified.
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
  });
});
