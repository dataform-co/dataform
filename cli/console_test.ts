import { expect } from "chai";
import Long from "long";

import { printExecutionGraph } from "df/cli/console";
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
