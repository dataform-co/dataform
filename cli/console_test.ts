import { expect } from "chai";
import Long from "long";

import {
  compiledGraphOutputType,
  dotRepresentation,
  escapeDotString,
  printCompiledGraph,
  printExecutionGraph,
} from "df/cli/console";
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
    expect(dotRepresentation(created)).equals(expected);
    // After a protobuf round-trip: the absent enumType defaults to UNKNOWN_TYPE.
    const decoded = dataform.CompiledGraph.decode(dataform.CompiledGraph.encode(created).finish());
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
    expect(dotRepresentation(decoded)).equals(expected);
    // The graph passed in isn't modified.
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
  });

  test("escapeDotString escapes quotes, backslashes and control characters", () => {
    expect(escapeDotString("plain.target_1")).equals("plain.target_1");
    expect(escapeDotString('a"b\\c\\')).equals('a\\"b\\\\c\\\\');
    expect(escapeDotString("line1\nline2\r\t\u0000\u001b\u007f\u0085\u2028\u2029")).equals(
      "line1\\\\nline2\\\\r\\\\t\\\\u0000\\\\u001b\\\\u007f\\\\u0085\\\\u2028\\\\u2029",
    );
  });

  test("escapes quotes, backslashes, Graphviz macros and control characters in node IDs, labels and edges", () => {
    const graph = dataform.CompiledGraph.create({
      tables: [
        {
          target: { database: "p", schema: "ds", name: 'my"table\\trail\\' },
          type: "table",
        },
      ],
      assertions: [
        {
          target: { database: "p", schema: "ds", name: "check\nrow\r\t\u0000" },
          dependencyTargets: [{ database: "p", schema: "ds", name: 'my"table\\trail\\' }],
        },
      ],
      operations: [
        {
          target: { database: "p", schema: "ds", name: "op\\N\u2028end" },
          dependencyTargets: [{ database: "p", schema: "ds", name: "check\nrow\r\t\u0000" }],
        },
      ],
    });

    expect(dotRepresentation(graph)).equals(
      [
        "digraph {",
        '"ds.my\\"table\\\\trail\\\\" [label="ds.my\\"table\\\\trail\\\\ [table]"];',
        '"ds.check\\\\nrow\\\\r\\\\t\\\\u0000" [label="ds.check\\\\nrow\\\\r\\\\t\\\\u0000"];',
        '"ds.op\\\\N\\\\u2028end" [label="ds.op\\\\N\\\\u2028end"];',
        '"ds.my\\"table\\\\trail\\\\" -> "ds.check\\\\nrow\\\\r\\\\t\\\\u0000";',
        '"ds.check\\\\nrow\\\\r\\\\t\\\\u0000" -> "ds.op\\\\N\\\\u2028end";',
        "}",
      ].join("\n"),
    );
  });

  test("printCompiledGraph prints DOT without ANSI escape sequences on an interactive terminal", () => {
    const graph = dataform.CompiledGraph.create({
      tables: [{ target: { database: "p", schema: "ds", name: "orders" }, type: "view" }],
      assertions: [
        {
          target: { database: "p", schema: "ds", name: "orders_assertion" },
          dependencyTargets: [{ database: "p", schema: "ds", name: "orders" }],
        },
      ],
      operations: [
        {
          target: { database: "p", schema: "ds", name: "orders_op" },
          dependencyTargets: [{ database: "p", schema: "ds", name: "orders_assertion" }],
        },
      ],
    });

    const expected = [
      "digraph {",
      '"ds.orders" [label="ds.orders [view]"];',
      '"ds.orders_assertion" [label="ds.orders_assertion"];',
      '"ds.orders_op" [label="ds.orders_op"];',
      '"ds.orders" -> "ds.orders_assertion";',
      '"ds.orders_assertion" -> "ds.orders_op";',
      "}",
    ].join("\n");

    expect(dotRepresentation(graph)).equals(expected);

    // Make stdout look like an interactive terminal, where target names are otherwise colored.
    const chunks: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    const originalIsTty = Object.getOwnPropertyDescriptor(process.stdout, "isTTY");
    const originalTerm = process.env.TERM;
    const originalCi = process.env.CI;
    process.stdout.write = ((chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    }) as typeof process.stdout.write;
    Object.defineProperty(process.stdout, "isTTY", { value: true, configurable: true });
    process.env.TERM = "xterm";
    delete process.env.CI;
    try {
      printCompiledGraph(graph, compiledGraphOutputType.Dot);
    } finally {
      process.stdout.write = originalWrite;
      if (originalIsTty) {
        Object.defineProperty(process.stdout, "isTTY", originalIsTty);
      } else {
        Reflect.deleteProperty(process.stdout, "isTTY");
      }
      if (originalTerm === undefined) {
        delete process.env.TERM;
      } else {
        process.env.TERM = originalTerm;
      }
      if (originalCi !== undefined) {
        process.env.CI = originalCi;
      }
    }
    expect(chunks.join("")).equals(`${expected}\n`);
  });

  test("omits bare semicolons when edges are empty or the graph is empty", () => {
    expect(dotRepresentation(dataform.CompiledGraph.create({}))).equals("digraph {\n}");

    const standaloneNodesGraph = dataform.CompiledGraph.create({
      tables: [{ target: { database: "p", schema: "ds", name: "t1" }, type: "table" }],
      assertions: [{ target: { database: "p", schema: "ds", name: "a1" } }],
      operations: [{ target: { database: "p", schema: "ds", name: "o1" } }],
    });
    expect(dotRepresentation(standaloneNodesGraph)).equals(
      [
        "digraph {",
        '"ds.t1" [label="ds.t1 [table]"];',
        '"ds.a1" [label="ds.a1"];',
        '"ds.o1" [label="ds.o1"];',
        "}",
      ].join("\n"),
    );
  });
});
