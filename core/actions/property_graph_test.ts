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
      defaultSchema: "defaultDataset"
    })
  );
}

function compile(config: any, filename = "definitions/graph.yaml"): dataform.PropertyGraph {
  return new PropertyGraph(makeSession(), config, filename).compile();
}

suite("property_graph", () => {
  test("compiles a minimal graph with one entity and no relationships", () => {
    const compiled = compile({
      name: "Simple",
      entities: [
        {
          name: "Account",
          dataSourceString: "proj.ds.Accounts",
          keys: ["id"]
        }
      ]
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "Simple"
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "Simple"
        },
        fileName: "definitions/graph.yaml",
        entities: [
          {
            name: "Account",
            dataSource: { database: "proj", schema: "ds", name: "Accounts" },
            keys: ["id"]
          }
        ],
        graphBody: "NODE TABLES (\n  `proj.ds.Accounts` AS Account KEY (id)\n)"
      })
    );
  });

  test("normalizes scalar keys to a single-element list", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "A", dataSourceString: "p.d.A", keys: "id" }
      ]
    });

    expect(compiled.entities[0].keys).deep.equals(["id"]);
  });

  test("parses 3-part dataSourceString into database.schema.name", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "A", dataSourceString: "myProj.myDs.MyTable", keys: ["id"] }
      ]
    });

    expect(asPlainObject(compiled.entities[0].dataSource)).deep.equals({
      database: "myProj",
      schema: "myDs",
      name: "MyTable"
    });
  });

  test("resolves dataSourceDataset map form with default project fallback", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "A",
          dataSourceDataset: { dataset: "customDs", table: "MyTable" },
          keys: ["id"]
        }
      ]
    });

    expect(asPlainObject(compiled.entities[0].dataSource)).deep.equals({
      database: "defaultProject",
      schema: "customDs",
      name: "MyTable"
    });
  });

  test("synthesizes default label named after entity when root-level fields set", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: [{ name: "balance", expression: "balance" }]
        }
      ]
    });

    expect(compiled.entities[0].labels).length(1);
    expect(compiled.entities[0].labels[0].name).equals("Account");
    expect(compiled.entities[0].labels[0].fields).length(1);
    expect(compiled.entities[0].labels[0].fields[0].name).equals("balance");
  });

  test("expands field shorthand strings into {name, expression} objects", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fields: ["balance", "owner"]
        }
      ]
    });

    const fields = compiled.entities[0].labels[0].fields;
    expect(fields.map(f => ({ name: f.name, expression: f.expression }))).deep.equals([
      { name: "balance", expression: "balance" },
      { name: "owner", expression: "owner" }
    ]);
  });

  test("fieldWildcard.importAll renders as PROPERTIES ARE ALL COLUMNS", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fieldWildcard: { importAll: true }
        }
      ]
    });

    expect(compiled.graphBody).contains("PROPERTIES ARE ALL COLUMNS");
    expect(compiled.graphBody).not.contains("EXCEPT");
  });

  test("fieldWildcard with except renders as PROPERTIES ARE ALL COLUMNS EXCEPT (...)", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "A",
          dataSourceString: "p.d.A",
          keys: ["id"],
          fieldWildcard: { importAll: true, except: ["secret", "internal"] }
        }
      ]
    });

    expect(compiled.graphBody).contains(
      "PROPERTIES ARE ALL COLUMNS EXCEPT (secret, internal)"
    );
  });

  test("multiple explicit labels render in declared order", () => {
    const compiled = compile({
      name: "G",
      entities: [
        {
          name: "Account",
          dataSourceString: "p.d.A",
          keys: ["id"],
          labels: [
            { name: "Account", fields: [{ name: "id", expression: "id" }] },
            { name: "Auditable", fields: [{ name: "createdAt", expression: "created_at" }] }
          ]
        }
      ]
    });

    expect(compiled.entities[0].labels.map(l => l.name)).deep.equals(["Account", "Auditable"]);
    const fragment = compiled.graphBody;
    expect(fragment.indexOf("LABEL Account")).below(fragment.indexOf("LABEL Auditable"));
  });

  test("relationship endpoints render SOURCE/DESTINATION KEY REFERENCES clauses", () => {
    const compiled = compile({
      name: "FinGraph",
      entities: [
        { name: "Account", dataSourceString: "p.d.Accounts", keys: ["id"] }
      ],
      relationships: [
        {
          name: "Transfer",
          dataSourceString: "p.d.Transfers",
          source: {
            entity: "Account",
            joinKeys: { relationshipColumns: ["sender_id"], entityColumns: ["id"] }
          },
          destination: {
            entity: "Account",
            joinKeys: { relationshipColumns: ["receiver_id"], entityColumns: ["id"] }
          }
        }
      ]
    });

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "FinGraph"
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "FinGraph"
        },
        fileName: "definitions/graph.yaml",
        entities: [
          {
            name: "Account",
            dataSource: { database: "p", schema: "d", name: "Accounts" },
            keys: ["id"]
          }
        ],
        relationships: [
          {
            name: "Transfer",
            dataSource: { database: "p", schema: "d", name: "Transfers" },
            source: {
              entity: "Account",
              relationshipColumns: ["sender_id"],
              entityColumns: ["id"]
            },
            destination: {
              entity: "Account",
              relationshipColumns: ["receiver_id"],
              entityColumns: ["id"]
            }
          }
        ],
        graphBody:
          "NODE TABLES (\n  `p.d.Accounts` AS Account KEY (id)\n)\n" +
          "EDGE TABLES (\n  `p.d.Transfers` AS Transfer " +
          "SOURCE KEY (sender_id) REFERENCES Account (id) " +
          "DESTINATION KEY (receiver_id) REFERENCES Account (id)\n)"
      })
    );
  });

  test("endpoint entity_columns default to referenced entity keys when omitted", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "Account", dataSourceString: "p.d.A", keys: ["id"] }
      ],
      relationships: [
        {
          name: "SelfLink",
          dataSourceString: "p.d.Links",
          source: {
            entity: "Account",
            joinKeys: { relationshipColumns: ["from_id"] }
          },
          destination: {
            entity: "Account",
            joinKeys: { relationshipColumns: ["to_id"] }
          }
        }
      ]
    });

    expect(compiled.relationships[0].source.entityColumns).deep.equals(["id"]);
    expect(compiled.relationships[0].destination.entityColumns).deep.equals(["id"]);
  });

  test("every emitted NODE and EDGE table has explicit AS alias", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "Account", dataSourceString: "p.d.Accounts", keys: ["id"] },
        { name: "Person", dataSourceString: "p.d.Persons", keys: ["id"] }
      ],
      relationships: [
        {
          name: "Owns",
          dataSourceString: "p.d.Ownership",
          source: {
            entity: "Person",
            joinKeys: { relationshipColumns: ["person_id"] }
          },
          destination: {
            entity: "Account",
            joinKeys: { relationshipColumns: ["account_id"] }
          }
        }
      ]
    });

    expect(compiled.graphBody).contains("`p.d.Accounts` AS Account");
    expect(compiled.graphBody).contains("`p.d.Persons` AS Person");
    expect(compiled.graphBody).contains("`p.d.Ownership` AS Owns");
  });

  test("errors when graph name is missing", () => {
    expect(() =>
      compile({
        entities: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }]
      })
    ).to.throw("Property graphs must have a populated 'name' field.");
  });

  test("errors when entities list is empty", () => {
    expect(() => compile({ name: "G", entities: [] })).to.throw(
      "Property graph 'G' must declare at least one entity."
    );
  });

  test("errors when entity has no data source", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [{ name: "A", keys: ["id"] }]
      })
    ).to.throw("entity 'A': must declare a data source.");
  });

  test("errors when entity has no keys", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [{ name: "A", dataSourceString: "p.d.A" }]
      })
    ).to.throw("entity 'A' must declare 'keys'.");
  });

  test("errors when entity has both root-level fields and labels list", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          {
            name: "A",
            dataSourceString: "p.d.A",
            keys: ["id"],
            fields: [{ name: "x", expression: "x" }],
            labels: [{ name: "A", fields: [{ name: "y", expression: "y" }] }]
          }
        ]
      })
    ).to.throw("cannot combine root-level 'fields'/'fieldWildcard' with a 'labels' list");
  });

  test("errors when relationship is missing source", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
        relationships: [
          {
            name: "R",
            dataSourceString: "p.d.R",
            destination: {
              entity: "A",
              joinKeys: { relationshipColumns: ["a_id"] }
            }
          }
        ]
      })
    ).to.throw("relationship 'R' must declare 'source'.");
  });

  test("errors when endpoint references an unknown entity", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [{ name: "A", dataSourceString: "p.d.A", keys: ["id"] }],
        relationships: [
          {
            name: "R",
            dataSourceString: "p.d.R",
            source: {
              entity: "Ghost",
              joinKeys: { relationshipColumns: ["x"] }
            },
            destination: {
              entity: "A",
              joinKeys: { relationshipColumns: ["y"] }
            }
          }
        ]
      })
    ).to.throw("relationship 'R' source references unknown entity 'Ghost'.");
  });

  test("errors on duplicate entity names", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          { name: "A", dataSourceString: "p.d.A1", keys: ["id"] },
          { name: "A", dataSourceString: "p.d.A2", keys: ["id"] }
        ]
      })
    ).to.throw("duplicate entity name 'A'");
  });

  test("rejects dataSourceCatalog variant", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          {
            name: "A",
            dataSourceCatalog: {
              project: "p",
              catalog: "cat",
              namespace: "ns",
              table: "T"
            },
            keys: ["id"]
          }
        ]
      })
    ).to.throw("Catalog-based data sources");
  });

  test("rejects 4-part dataSourceString as catalog reference", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          {
            name: "A",
            dataSourceString: "proj.cat.ns.tbl",
            keys: ["id"]
          }
        ]
      })
    ).to.throw("Catalog-based data sources");
  });

  test("rejects 2-part dataSourceString as invalid path", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          { name: "A", dataSourceString: "ds.tbl", keys: ["id"] }
        ]
      })
    ).to.throw("must be 'project.dataset.table'");
  });

  test("preserves composite keys list", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "A", dataSourceString: "p.d.A", keys: ["id", "date"] }
      ]
    });

    expect(compiled.entities[0].keys).deep.equals(["id", "date"]);
    expect(compiled.graphBody).contains("KEY (id, date)");
  });

  test("joinKeys array shorthand expands to relationshipColumns with defaulted entityColumns", () => {
    const compiled = compile({
      name: "G",
      entities: [
        { name: "Account", dataSourceString: "p.d.A", keys: ["id"] }
      ],
      relationships: [
        {
          name: "Link",
          dataSourceString: "p.d.L",
          source: { entity: "Account", joinKeys: ["from_id"] },
          destination: { entity: "Account", joinKeys: ["to_id"] }
        }
      ]
    });

    expect(compiled.relationships[0].source.relationshipColumns).deep.equals(["from_id"]);
    expect(compiled.relationships[0].source.entityColumns).deep.equals(["id"]);
    expect(compiled.relationships[0].destination.relationshipColumns).deep.equals(["to_id"]);
    expect(compiled.relationships[0].destination.entityColumns).deep.equals(["id"]);
  });

  test("errors when label declares neither fields nor a wildcard", () => {
    expect(() =>
      compile({
        name: "G",
        entities: [
          {
            name: "A",
            dataSourceString: "p.d.A",
            keys: ["id"],
            labels: [{ name: "Empty" }]
          }
        ]
      })
    ).to.throw("must declare at least one field or a wildcard");
  });
});
