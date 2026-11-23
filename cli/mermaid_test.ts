import { expect } from "chai";

import { mermaidNodeIds, mermaidRepresentation, mermaidSizeWarning } from "df/cli/mermaid";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("mermaidRepresentation", () => {
  // Expected IDs are `n` + the first 8 hex characters of the SHA-256 of the target key
  // (JSON `["database","schema","name"]`), computed independently of the implementation.
  test("renders nodes sorted by target and edges for tables, assertions and operations", () => {
    const graph = dataform.CompiledGraph.create({
      operations: [
        {
          target: { database: "proj", schema: "ds", name: "op" },
          dependencyTargets: [
            { database: "proj", schema: "other", name: "declared" },
            { database: "proj", schema: "ds", name: "end" },
          ],
        },
      ],
      assertions: [
        {
          target: { database: "proj", schema: "ds", name: "end_assert" },
          dependencyTargets: [{ database: "proj", schema: "ds", name: "end" }],
        },
      ],
      tables: [
        {
          target: { database: "proj", schema: "ds", name: "end" },
          enumType: dataform.TableType.VIEW,
          dependencyTargets: [{ database: "proj", schema: "ds", name: "source" }],
        },
        {
          target: { database: "proj", schema: "ds", name: "source" },
          enumType: dataform.TableType.TABLE,
        },
      ],
    });

    expect(mermaidRepresentation(graph)).equals(
      [
        "graph LR",
        '  nf23acaab["ds.end [view]"]',
        '  n27646b3e["ds.end_assert"]',
        '  n83980845["ds.op"]',
        '  nba5afcb5["ds.source [table]"]',
        '  n771fb3e5["other.declared"]',
        "  nf23acaab --> n27646b3e",
        "  nf23acaab --> n83980845",
        "  nba5afcb5 --> nf23acaab",
        "  n771fb3e5 --> n83980845",
      ].join("\n"),
    );
  });

  test("output does not depend on the order of actions in the graph", () => {
    const tables = [
      {
        target: { database: "proj", schema: "ds", name: "downstream" },
        enumType: dataform.TableType.TABLE,
        dependencyTargets: [{ database: "proj", schema: "ds", name: "upstream" }],
      },
      {
        target: { database: "proj", schema: "ds", name: "upstream" },
        enumType: dataform.TableType.INCREMENTAL,
      },
    ];
    const expected = [
      "graph LR",
      '  n969c247f["ds.downstream [table]"]',
      '  n8e4731e5["ds.upstream [incremental]"]',
      "  n8e4731e5 --> n969c247f",
    ].join("\n");

    expect(mermaidRepresentation(dataform.CompiledGraph.create({ tables }))).equals(expected);
    expect(
      mermaidRepresentation(dataform.CompiledGraph.create({ tables: [...tables].reverse() })),
    ).equals(expected);
  });

  test("keeps same schema.name in different databases distinct and labels them with the database", () => {
    const graph = dataform.CompiledGraph.create({
      tables: [
        {
          target: { database: "b", schema: "ds", name: "t" },
          enumType: dataform.TableType.TABLE,
          dependencyTargets: [{ database: "a", schema: "ds", name: "t" }],
        },
      ],
      operations: [{ target: { database: "a", schema: "ds", name: "op" } }],
    });

    expect(mermaidRepresentation(graph)).equals(
      [
        "graph LR",
        '  n641ca9f6["ds.op"]',
        '  nbe79043f["a.ds.t"]',
        '  n4935ea06["b.ds.t [table]"]',
        "  nbe79043f --> n4935ea06",
      ].join("\n"),
    );
  });

  test("doesn't label a target without a database with an empty database", () => {
    // E.g. a project with no defaultDatabase whose action shares its schema.name with a
    // declaration in another project.
    const graph = dataform.CompiledGraph.create({
      tables: [
        {
          target: { schema: "analytics", name: "users" },
          enumType: dataform.TableType.TABLE,
          dependencyTargets: [{ database: "raw_proj", schema: "analytics", name: "users" }],
        },
      ],
    });

    expect(mermaidRepresentation(graph)).equals(
      [
        "graph LR",
        '  n1de98535["analytics.users [table]"]',
        '  n42c7f6fe["raw_proj.analytics.users"]',
        "  n42c7f6fe --> n1de98535",
      ].join("\n"),
    );
  });

  test("labels tables in graphs that only set the string type", () => {
    const tables = [
      { target: { database: "p", schema: "ds", name: "inc" }, type: "incremental" },
      { target: { database: "p", schema: "ds", name: "t" }, type: "table" },
    ];
    const expected = [
      "graph LR",
      '  n06d991ab["ds.inc [incremental]"]',
      '  n2e6b04ee["ds.t [table]"]',
    ].join("\n");

    // As created in memory: enumType is unset.
    const created = dataform.CompiledGraph.create({ tables });
    expect(mermaidRepresentation(created)).equals(expected);
    // After a protobuf round-trip: the absent enumType defaults to UNKNOWN_TYPE.
    const decoded = dataform.CompiledGraph.decode(dataform.CompiledGraph.encode(created).finish());
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
    expect(mermaidRepresentation(decoded)).equals(expected);
    // The graph passed in isn't modified.
    expect(decoded.tables[0].enumType).equals(dataform.TableType.UNKNOWN_TYPE);
  });

  test("escapes characters Mermaid would interpret in labels", () => {
    const labels = [
      'we"ird',
      "we#quot;ird",
      "a<br>b",
      "x &amp; y",
      "`md` *x*",
      "a\\nb",
      "fa:fa-user",
      "a\nb",
      "tab\there",
      "del\u007f",
      "ls\u2028ps\u2029nel\u0085",
      "cost$$x+1$$",
    ].map((name) => {
      const graph = dataform.CompiledGraph.create({
        operations: [{ target: { database: "p", schema: "ds", name } }],
      });
      return mermaidRepresentation(graph)
        .split("\n")[1]
        .replace(/^.*\["(.*)"\]$/, "$1");
    });

    expect(labels).deep.equals([
      "ds.we#34;ird",
      "ds.we#35;quot;ird",
      "ds.a#60;br#62;b",
      "ds.x #38;amp; y",
      "ds.#96;md#96; *x*",
      "ds.a#92;nb",
      "ds.fa#58;fa-user",
      "ds.a\u240ab",
      "ds.tab\u2409here",
      "ds.del\u2421",
      "ds.ls\u2424ps\u2424nel\u2424",
      "ds.cost#36;#36;x+1#36;#36;",
    ]);
  });

  test("deduplicates and sorts edges regardless of dependency order", () => {
    const dep = (database: string, name: string) => ({ database, schema: "ds", name });
    const graph = (dependencyTargets: dataform.ITarget[]) =>
      mermaidRepresentation(
        dataform.CompiledGraph.create({
          operations: [{ target: dep("p", "op"), dependencyTargets }],
        }),
      );

    const expected = graph([dep("p", "a"), dep("q", "a")]);
    expect(graph([dep("q", "a"), dep("p", "a"), dep("q", "a"), dep("p", "a")])).equals(expected);
    expect(expected.split("\n").filter((line) => line.includes("-->"))).deep.equals([
      "  ned54cdbe --> n13bd00c9",
      "  n761050d2 --> n13bd00c9",
    ]);
  });

  test("renders an empty graph", () => {
    expect(mermaidRepresentation(dataform.CompiledGraph.create({}))).equals("graph LR");
  });
});

suite("mermaidNodeIds", () => {
  const key = (database: string, schema: string, name: string) =>
    JSON.stringify([database, schema, name]);

  test("uses n plus 8 hex characters of the key's hash", () => {
    const keys = [key("proj", "ds", "end"), key("", "ds", "t"), key("my-proj", "ds", "café x")];
    const ids = mermaidNodeIds(keys);
    expect(ids.get(keys[0])).equals("nf23acaab");
    expect(ids.get(keys[1])).equals("nafd11e1f");
    keys.forEach((k) => expect(ids.get(k)).to.match(/^n[0-9a-f]{8}$/));
  });

  test("lengthens only IDs whose 8-character prefixes collide", () => {
    // These two keys' SHA-256 hashes both start with b9d7d467.
    const colliding = [key("p", "ds", "c96056"), key("p", "ds", "c140289")];
    const other = key("proj", "ds", "end");
    const ids = mermaidNodeIds([...colliding, other]);
    expect(ids.get(colliding[0])).equals("nb9d7d4679");
    expect(ids.get(colliding[1])).equals("nb9d7d467b");
    expect(ids.get(other)).equals("nf23acaab");
    // Adding a colliding node doesn't change unrelated nodes' IDs.
    expect(mermaidNodeIds([colliding[0], other]).get(other)).equals("nf23acaab");
    expect(mermaidNodeIds([colliding[0], other]).get(colliding[0])).equals("nb9d7d467");
  });
});

suite("mermaidSizeWarning", () => {
  const chain = (tables: number) =>
    mermaidRepresentation(
      dataform.CompiledGraph.create({
        tables: Array.from({ length: tables }, (_, i) => ({
          target: { database: "p", schema: "ds", name: `t${i}` },
          enumType: dataform.TableType.TABLE,
          dependencyTargets: i === 0 ? [] : [{ database: "p", schema: "ds", name: `t${i - 1}` }],
        })),
      }),
    );

  test("no warning for a diagram within Mermaid's limits", () => {
    expect(mermaidSizeWarning(chain(501))).equals(undefined);
  });

  test("warns above 500 edges", () => {
    expect(mermaidSizeWarning(chain(502))).contains("has 501 edges");
  });

  test("warns only above exactly 50,000 characters", () => {
    const text = (length: number) => "graph LR\n" + "x".repeat(length - "graph LR\n".length);
    expect(mermaidSizeWarning(text(50000))).equals(undefined);
    expect(mermaidSizeWarning(text(50001))).contains("50001 characters");
  });

  test("warns above 50,000 characters", () => {
    const longLabels = mermaidRepresentation(
      dataform.CompiledGraph.create({
        operations: Array.from({ length: 300 }, (_, i) => ({
          target: { database: "p", schema: "ds", name: `${"x".repeat(200)}${i}` },
        })),
      }),
    );
    expect(longLabels.length).greaterThan(50000);
    expect(mermaidSizeWarning(longLabels)).contains("0 edges");
  });
});
