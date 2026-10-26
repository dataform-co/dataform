import { expect } from "chai";

import { PropertyGraph } from "df/core/actions/property_graph";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";

function makeSession(): Session {
  return new Session(
    "/tmp/root",
    dataform.ProjectConfig.create({
      defaultDatabase: "defaultProject",
      defaultSchema: "defaultDataset",
    }),
  );
}

function compile(config: any, filename = "definitions/graph.yaml"): dataform.PropertyGraph {
  const action = new PropertyGraph(makeSession(), config, filename);
  const compiled = action.compile();
  action.finalize();
  return compiled;
}

function build(config: any, filename = "definitions/graph.yaml"): PropertyGraph {
  return new PropertyGraph(makeSession(), config, filename);
}

const graphTarget = (name: string) => ({
  database: "defaultProject",
  schema: "defaultDataset",
  name,
});

suite("property_graph", () => {
  test("compiles a minimal graph with one node and no edges", () => {
    const compiled = compile({
      name: "Simple",
      nodes: [
        {
          name: "Account",
          dataSourceString: "proj.ds.Accounts",
          keys: ["id"],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("Simple"),
        canonicalTarget: graphTarget("Simple"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "proj", schema: "ds", name: "Accounts" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `proj.ds.Accounts` AS Account KEY (id)\n)",
      }),
    );
  });

  test("propagates tags into the compiled proto", () => {
    const compiled = compile({
      name: "Tagged",
      tags: ["nightly", "reporting"],
      nodes: [
        {
          name: "Account",
          dataSourceString: "proj.ds.Accounts",
          keys: ["id"],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("Tagged"),
        canonicalTarget: graphTarget("Tagged"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        tags: ["nightly", "reporting"],
        nodes: [
          {
            name: "Account",
            dataSource: { database: "proj", schema: "ds", name: "Accounts" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `proj.ds.Accounts` AS Account KEY (id)\n)",
      }),
    );
  });

  test("normalizes scalar keys to a single-element list", () => {
    const compiled = compile({
      name: "G",
      nodes: [{ name: "A", dataSourceString: "p.d.A", keys: "id" }],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS A KEY (id)\n)",
      }),
    );
  });

  test("parses 3-part dataSourceString into database.schema.name", () => {
    const compiled = compile({
      name: "G",
      nodes: [{ name: "A", dataSourceString: "myProj.myDs.MyTable", keys: ["id"] }],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "myProj", schema: "myDs", name: "MyTable" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `myProj.myDs.MyTable` AS A KEY (id)\n)",
      }),
    );
  });

  test("resolves dataSourceDataset map form with default project fallback", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceDataset: { dataset: "customDs", table: "MyTable" },
          keys: ["id"],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "defaultProject", schema: "customDs", name: "MyTable" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `defaultProject.customDs.MyTable` AS A KEY (id)\n)",
      }),
    );
  });

  test("synthesizes default label named after node when root-level fields set", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: [{ name: "balance", expression: "balance" }],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "Account",
                description: "",
                fields: [{ name: "balance", expression: "balance" }],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id) DEFAULT LABEL PROPERTIES (balance)\n)",
      }),
    );
  });

  test("expands field shorthand strings into {name, expression} objects", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: ["balance", "owner"],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "",
                fields: [
                  { name: "balance", expression: "balance" },
                  { name: "owner", expression: "owner" },
                ],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL PROPERTIES (balance, owner)\n)",
      }),
    );
  });

  test("fieldWildcard.importAll renders as PROPERTIES ARE ALL COLUMNS", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fieldWildcard: { importAll: true },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [{ name: "A", description: "", importAll: true, isDefault: true }],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL PROPERTIES ARE ALL COLUMNS\n)",
      }),
    );
  });

  test("fieldWildcard with except renders as PROPERTIES ARE ALL COLUMNS EXCEPT (...)", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fieldWildcard: { importAll: true, except: ["secret", "internal"] },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "",
                importAll: true,
                importExcept: ["secret", "internal"],
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL " +
          "PROPERTIES ARE ALL COLUMNS EXCEPT (secret, internal)\n)",
      }),
    );
  });

  test("multiple explicit labels render in declared order", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          labels: [
            { name: "Account", fields: [{ name: "id", expression: "id" }] },
            { name: "Auditable", fields: [{ name: "createdAt", expression: "created_at" }] },
          ],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "Account",
                fields: [{ name: "id", expression: "id" }],
                importAll: false,
                isDefault: false,
              },
              {
                name: "Auditable",
                fields: [{ name: "createdAt", expression: "created_at" }],
                importAll: false,
                isDefault: false,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id) " +
          "LABEL Account PROPERTIES (id) " +
          "LABEL Auditable PROPERTIES (created_at AS createdAt)\n)",
      }),
    );
  });

  test("edge endpoints render SOURCE/DESTINATION KEY REFERENCES clauses", () => {
    const compiled = compile({
      name: "FinGraph",
      nodes: [{ name: "Account", dataSourceString: "p.d.Accounts", keys: ["id"] }],
      edges: [
        {
          name: "Transfer",
          dataSourceString: "p.d.Transfers",
          source: {
            node: "Account",
            joinKeys: { edgeColumns: ["sender_id"], nodeColumns: ["id"] },
          },
          destination: {
            node: "Account",
            joinKeys: { edgeColumns: ["receiver_id"], nodeColumns: ["id"] },
          },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("FinGraph"),
        canonicalTarget: graphTarget("FinGraph"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "Accounts" },
            keys: ["id"],
          },
        ],
        edges: [
          {
            name: "Transfer",
            dataSource: { database: "p", schema: "d", name: "Transfers" },
            source: {
              node: "Account",
              edgeColumns: ["sender_id"],
              nodeColumns: ["id"],
            },
            destination: {
              node: "Account",
              edgeColumns: ["receiver_id"],
              nodeColumns: ["id"],
            },
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.Accounts` AS Account KEY (id)\n)\n" +
          "EDGE TABLES (\n  `p.d.Transfers` AS Transfer " +
          "SOURCE KEY (sender_id) REFERENCES Account (id) " +
          "DESTINATION KEY (receiver_id) REFERENCES Account (id)\n)",
      }),
    );
  });

  test("endpoint node_columns default to referenced node keys when omitted", () => {
    const compiled = compile({
      name: "G",
      nodes: [{ name: "Account", dataSourceString: "p.d.A", keys: ["id"] }],
      edges: [
        {
          name: "SelfLink",
          dataSourceString: "p.d.Links",
          source: {
            node: "Account",
            joinKeys: { edgeColumns: ["from_id"] },
          },
          destination: {
            node: "Account",
            joinKeys: { edgeColumns: ["to_id"] },
          },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
          },
        ],
        edges: [
          {
            name: "SelfLink",
            dataSource: { database: "p", schema: "d", name: "Links" },
            source: {
              node: "Account",
              edgeColumns: ["from_id"],
              nodeColumns: ["id"],
            },
            destination: {
              node: "Account",
              edgeColumns: ["to_id"],
              nodeColumns: ["id"],
            },
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id)\n)\n" +
          "EDGE TABLES (\n  `p.d.Links` AS SelfLink " +
          "SOURCE KEY (from_id) REFERENCES Account (id) " +
          "DESTINATION KEY (to_id) REFERENCES Account (id)\n)",
      }),
    );
  });

  test("every emitted NODE and EDGE table has explicit AS alias", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        { name: "Account", dataSourceString: "p.d.Accounts", keys: ["id"] },
        { name: "Person", dataSourceString: "p.d.Persons", keys: ["id"] },
      ],
      edges: [
        {
          name: "Owns",
          dataSourceString: "p.d.Ownership",
          source: {
            node: "Person",
            joinKeys: { edgeColumns: ["person_id"] },
          },
          destination: {
            node: "Account",
            joinKeys: { edgeColumns: ["account_id"] },
          },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "Accounts" },
            keys: ["id"],
          },
          {
            name: "Person",
            dataSource: { database: "p", schema: "d", name: "Persons" },
            keys: ["id"],
          },
        ],
        edges: [
          {
            name: "Owns",
            dataSource: { database: "p", schema: "d", name: "Ownership" },
            source: {
              node: "Person",
              edgeColumns: ["person_id"],
              nodeColumns: ["id"],
            },
            destination: {
              node: "Account",
              edgeColumns: ["account_id"],
              nodeColumns: ["id"],
            },
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.Accounts` AS Account KEY (id),\n" +
          "  `p.d.Persons` AS Person KEY (id)\n)\n" +
          "EDGE TABLES (\n  `p.d.Ownership` AS Owns " +
          "SOURCE KEY (person_id) REFERENCES Person (id) " +
          "DESTINATION KEY (account_id) REFERENCES Account (id)\n)",
      }),
    );
  });

  test("errors when graph name is missing", () => {
    expect(() =>
      compile({
        nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
      }),
    ).to.throw("Property graphs must have a populated 'name' field.");
  });

  test("errors when nodes list is empty", () => {
    expect(() => compile({ name: "G", nodes: [] })).to.throw(
      "Property graph 'G' must declare at least one node.",
    );
  });

  test("errors when node has no data source", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [{ name: "A", keys: ["id"] }],
      }),
    ).to.throw("node 'A': must declare a data source.");
  });

  test("errors when node has no keys", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [{ name: "A", dataSourceString: "p.d.A" }],
      }),
    ).to.throw("node 'A' must declare 'keys'.");
  });

  test("errors when node has both root-level fields and labels list", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            dataSourceString: "p.d.A",
            keys: ["id"],
            fields: [{ name: "x", expression: "x" }],
            labels: [{ name: "A", fields: [{ name: "y", expression: "y" }] }],
          },
        ],
      }),
    ).to.throw("cannot combine root-level 'fields'/'fieldWildcard' with a 'labels' list");
  });

  test("errors when edge is missing source", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
        edges: [
          {
            name: "R",
            dataSourceString: "p.d.R",
            destination: {
              node: "A",
              joinKeys: { edgeColumns: ["a_id"] },
            },
          },
        ],
      }),
    ).to.throw("edge 'R' must declare 'source'.");
  });

  test("errors when endpoint references an unknown node", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
        edges: [
          {
            name: "R",
            dataSourceString: "p.d.R",
            source: {
              node: "Ghost",
              joinKeys: { edgeColumns: ["x"] },
            },
            destination: {
              node: "A",
              joinKeys: { edgeColumns: ["y"] },
            },
          },
        ],
      }),
    ).to.throw("edge 'R' source references unknown node 'Ghost'.");
  });

  test("errors on duplicate node names", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          { name: "A", dataSourceString: "p.d.A1", keys: ["id"] },
          { name: "A", dataSourceString: "p.d.A2", keys: ["id"] },
        ],
      }),
    ).to.throw("duplicate node name 'A'");
  });

  test("rejects dataSourceCatalog variant", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            dataSourceCatalog: {
              project: "p",
              catalog: "cat",
              namespace: "ns",
              table: "T",
            },
            keys: ["id"],
          },
        ],
      }),
    ).to.throw("Catalog-based data sources");
  });

  test("rejects 4-part dataSourceString as catalog reference", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            dataSourceString: "proj.cat.ns.tbl",
            keys: ["id"],
          },
        ],
      }),
    ).to.throw("Catalog-based data sources");
  });

  test("rejects 2-part dataSourceString as invalid path", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [{ name: "A", dataSourceString: "ds.tbl", keys: ["id"] }],
      }),
    ).to.throw("must be 'project.dataset.table'");
  });

  test("preserves composite keys list", () => {
    const compiled = compile({
      name: "G",
      nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id", "date"] }],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id", "date"],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS A KEY (id, date)\n)",
      }),
    );
  });

  test("joinKeys array shorthand expands to edgeColumns with defaulted nodeColumns", () => {
    const compiled = compile({
      name: "G",
      nodes: [{ name: "Account", dataSourceString: "p.d.A", keys: ["id"] }],
      edges: [
        {
          name: "Link",
          dataSourceString: "p.d.L",
          source: { node: "Account", joinKeys: ["from_id"] },
          destination: { node: "Account", joinKeys: ["to_id"] },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
          },
        ],
        edges: [
          {
            name: "Link",
            dataSource: { database: "p", schema: "d", name: "L" },
            source: {
              node: "Account",
              edgeColumns: ["from_id"],
              nodeColumns: ["id"],
            },
            destination: {
              node: "Account",
              edgeColumns: ["to_id"],
              nodeColumns: ["id"],
            },
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id)\n)\n" +
          "EDGE TABLES (\n  `p.d.L` AS Link " +
          "SOURCE KEY (from_id) REFERENCES Account (id) " +
          "DESTINATION KEY (to_id) REFERENCES Account (id)\n)",
      }),
    );
  });

  test("errors when label declares neither fields nor a wildcard", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            dataSourceString: "p.d.A",
            keys: ["id"],
            labels: [{ name: "Empty" }],
          },
        ],
      }),
    ).to.throw("must declare at least one of: 'fields', 'fieldWildcard', 'description'");
  });

  test("fields:{importAll:true} map form hoists to fieldWildcard", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: { importAll: true },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [{ name: "A", description: "", importAll: true, isDefault: true }],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL PROPERTIES ARE ALL COLUMNS\n)",
      }),
    );
  });

  test("fields:{importAll:true, except:[...]} map form hoists with except", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: { importAll: true, except: ["secret"] },
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "",
                importAll: true,
                importExcept: ["secret"],
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL " +
          "PROPERTIES ARE ALL COLUMNS EXCEPT (secret)\n)",
      }),
    );
  });

  test("synthesized default label sets isDefault=true and renders DEFAULT LABEL", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: [{ name: "balance", expression: "balance" }],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "Account",
                description: "",
                fields: [{ name: "balance", expression: "balance" }],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id) DEFAULT LABEL PROPERTIES (balance)\n)",
      }),
    );
  });

  test("explicitly configured labels set isDefault=false and render LABEL <name>", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          labels: [{ name: "Account", fields: [{ name: "id", expression: "id" }] }],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "Account",
                fields: [{ name: "id", expression: "id" }],
                importAll: false,
                isDefault: false,
              },
            ],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS Account KEY (id) LABEL Account PROPERTIES (id)\n)",
      }),
    );
  });

  test("errors when a non-default label declares description or synonyms", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "Account",
            dataSourceString: "p.d.A",
            keys: ["id"],
            labels: [
              {
                name: "Account",
                description: "customer accounts",
                fields: [{ name: "id", expression: "id" }],
              },
            ],
          },
        ],
      }),
    ).to.throw("only allowed on the DEFAULT label");

    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "Account",
            dataSourceString: "p.d.A",
            keys: ["id"],
            labels: [
              {
                name: "Account",
                synonyms: ["customer"],
                fields: [{ name: "id", expression: "id" }],
              },
            ],
          },
        ],
      }),
    ).to.throw("only allowed on the DEFAULT label");
  });

  test("field description and synonyms render as per-field OPTIONS(...)", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: [
            {
              name: "balance",
              expression: "balance",
              description: "current balance in USD",
              synonyms: ["amount"],
            },
          ],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "",
                fields: [
                  {
                    name: "balance",
                    expression: "balance",
                    description: "current balance in USD",
                    synonyms: ["amount"],
                  },
                ],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL PROPERTIES " +
          `(balance OPTIONS(description="current balance in USD", synonyms=["amount"]))\n)`,
      }),
    );
  });

  test("graph-level description is stored on the compiled proto but not emitted to graphBody", () => {
    const compiled = compile({
      name: "G",
      description: "high-value customer graph",
      nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        disabled: false,
        description: "high-value customer graph",
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS A KEY (id)\n)",
      }),
    );
  });

  test("node with only description synthesizes a DEFAULT LABEL with OPTIONS", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          description: "customer account entity",
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "Account",
                description: "customer account entity",
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS Account KEY (id) DEFAULT LABEL " +
          `OPTIONS(description="customer account entity")\n)`,
      }),
    );
  });

  test("string quoting in OPTIONS escapes embedded double quotes and backslashes", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          description: `has "quotes" and \\ backslash`,
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: `has "quotes" and \\ backslash`,
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL " +
          `OPTIONS(description="has \\"quotes\\" and \\\\ backslash")\n)`,
      }),
    );
  });

  test("string quoting in OPTIONS escapes newlines carriage returns and tabs", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          description: "line1\nline2\r\nline3\ttab",
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "line1\nline2\r\nline3\ttab",
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL " +
          `OPTIONS(description="line1\\nline2\\r\\nline3\\ttab")\n)`,
      }),
    );
  });

  test("label with name 'DEFAULT' is treated as DEFAULT LABEL", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          labels: [
            {
              name: "DEFAULT",
              description: "the default",
              synonyms: ["main"],
              fields: [{ name: "id", expression: "id" }],
            },
          ],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                description: "the default",
                synonyms: ["main"],
                fields: [{ name: "id", expression: "id" }],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL " +
          `OPTIONS(description="the default", synonyms=["main"]) PROPERTIES (id)\n)`,
      }),
    );
  });

  test("label name 'default' is case-insensitive and treated as DEFAULT LABEL", () => {
    const compiled = compile({
      name: "G",
      nodes: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          labels: [
            {
              name: "default",
              fields: [{ name: "id", expression: "id" }],
            },
          ],
        },
      ],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
            labels: [
              {
                name: "A",
                fields: [{ name: "id", expression: "id" }],
                importAll: false,
                isDefault: true,
              },
            ],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS A KEY (id) DEFAULT LABEL PROPERTIES (id)\n)",
      }),
    );
  });

  test("errors when more than one DEFAULT label is declared", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            dataSourceString: "p.d.A",
            keys: ["id"],
            labels: [
              { name: "DEFAULT", fields: [{ name: "id", expression: "id" }] },
              { name: "default", fields: [{ name: "id", expression: "id" }] },
            ],
          },
        ],
      }),
    ).to.throw("only one DEFAULT label is allowed");
  });

  test("disabled=true propagates onto the compiled PropertyGraph", () => {
    const compiled = compile({
      name: "G",
      disabled: true,
      nodes: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: true,
        nodes: [
          {
            name: "A",
            dataSource: { database: "p", schema: "d", name: "A" },
            keys: ["id"],
          },
        ],
        graphBody: "NODE TABLES (\n  `p.d.A` AS A KEY (id)\n)",
      }),
    );
  });

  test("scalar ref normalizes to dataSourceRef and populates dependencyTargets", () => {
    const compiled = build({
      name: "G",
      nodes: [
        {
          name: "A",
          ref: "books",
          keys: ["id"],
        },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [{ name: "books", includeDependentAssertions: false }],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [{ name: "A", keys: ["id"] }],
      }),
    );
  });

  test("object ref preserves schema and database in dependencyTargets", () => {
    const compiled = build({
      name: "G",
      nodes: [
        {
          name: "A",
          ref: { name: "books", schema: "analytics", database: "proj" },
          keys: ["id"],
        },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [
          {
            database: "proj",
            schema: "analytics",
            name: "books",
            includeDependentAssertions: false,
          },
        ],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [{ name: "A", keys: ["id"] }],
      }),
    );
  });

  test("duplicate refs across nodes dedupe in dependencyTargets", () => {
    const compiled = build({
      name: "G",
      nodes: [
        { name: "A", ref: "books", keys: ["id"] },
        { name: "B", ref: "books", keys: ["id"] },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [{ name: "books", includeDependentAssertions: false }],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          { name: "A", keys: ["id"] },
          { name: "B", keys: ["id"] },
        ],
      }),
    );
  });

  test("graph-level dependOnDependencyAssertions propagates to every ref dependency", () => {
    const compiled = build({
      name: "G",
      dependOnDependencyAssertions: true,
      nodes: [
        { name: "A", ref: "books", keys: ["id"] },
        { name: "B", ref: "authors", keys: ["id"] },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [
          { name: "books", includeDependentAssertions: true },
          { name: "authors", includeDependentAssertions: true },
        ],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          { name: "A", keys: ["id"] },
          { name: "B", keys: ["id"] },
        ],
      }),
    );
  });

  test("ref-level includeDependentAssertions propagates to that dependency only", () => {
    const compiled = build({
      name: "G",
      nodes: [
        { name: "A", ref: { name: "books", includeDependentAssertions: true }, keys: ["id"] },
        { name: "B", ref: "authors", keys: ["id"] },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [
          { name: "books", includeDependentAssertions: true },
          { name: "authors", includeDependentAssertions: false },
        ],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          { name: "A", keys: ["id"] },
          { name: "B", keys: ["id"] },
        ],
      }),
    );
  });

  test("ref-level includeDependentAssertions=false overrides graph-level dependOnDependencyAssertions=true", () => {
    const compiled = build({
      name: "G",
      dependOnDependencyAssertions: true,
      nodes: [
        { name: "A", ref: { name: "books", includeDependentAssertions: false }, keys: ["id"] },
        { name: "B", ref: "authors", keys: ["id"] },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [
          { name: "books", includeDependentAssertions: false },
          { name: "authors", includeDependentAssertions: true },
        ],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          { name: "A", keys: ["id"] },
          { name: "B", keys: ["id"] },
        ],
      }),
    );
  });

  test("ref on edge also normalizes and populates dependencyTargets", () => {
    const compiled = build({
      name: "G",
      nodes: [
        { name: "A", dataSourceString: "p.d.A", keys: ["id"] },
        { name: "B", dataSourceString: "p.d.B", keys: ["id"] },
      ],
      edges: [
        {
          name: "R",
          ref: "wrote",
          keys: ["a_id", "b_id"],
          source: { node: "A", joinKeys: ["a_id"] },
          destination: { node: "B", joinKeys: ["b_id"] },
        },
      ],
    }).compile();

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("G"),
        canonicalTarget: graphTarget("G"),
        dependencyTargets: [{ name: "wrote", includeDependentAssertions: false }],
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          { name: "A", dataSource: { database: "p", schema: "d", name: "A" }, keys: ["id"] },
          { name: "B", dataSource: { database: "p", schema: "d", name: "B" }, keys: ["id"] },
        ],
        edges: [
          {
            name: "R",
            keys: ["a_id", "b_id"],
            source: { node: "A", edgeColumns: ["a_id"], nodeColumns: ["id"] },
            destination: { node: "B", edgeColumns: ["b_id"], nodeColumns: ["id"] },
          },
        ],
      }),
    );
  });

  test("errors when ref has no name", () => {
    expect(() =>
      compile({
        name: "G",
        nodes: [
          {
            name: "A",
            ref: { schema: "s" },
            keys: ["id"],
          },
        ],
      }),
    ).to.throw("'ref' must include a 'name'");
  });

  test("propagates jobLabels and merges with defaultJobLabels", () => {
    const session = new Session(
      "/tmp/root",
      dataform.ProjectConfig.create({
        defaultDatabase: "defaultProject",
        defaultSchema: "defaultDataset",
        defaultJobLabels: {
          env: "dev",
          team: "graph_team",
        },
      }),
    );
    const action = new PropertyGraph(
      session,
      {
        name: "LabeledGraph",
        jobLabels: {
          env: "prod",
          cost_center: "analytics",
        },
        nodes: [
          {
            name: "Account",
            dataSourceString: "proj.ds.Accounts",
            keys: ["id"],
          },
        ],
      } as any,
      "definitions/graph.yaml",
    );
    const compiled = action.compile();
    expect(session.graphErrors.compilationErrors).to.deep.equal([]);
    expect(asPlainObject(compiled.actionDescriptor)).to.deep.equal({
      jobLabels: {
        env: "prod",
        team: "graph_team",
        cost_center: "analytics",
      },
    });
  });
});
