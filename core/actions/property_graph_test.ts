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

function makeModelConfig(model: any, topLevelExtra: any = {}): any {
  return {
    version: "0.2.0.dev0/google",
    semantic_model: [model],
    ...topLevelExtra,
  };
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
  test("compiles a minimal semantic_model with one entity and no relationships", () => {
    const compiled = compile(
      makeModelConfig({
        name: "Simple",
        entities: [
          {
            name: "Account",
            source: "bigquery:proj.ds.Accounts",
            primary_key: ["id"],
          },
        ],
      }),
    );

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
        graphBody:
          "NODE TABLES (\n  `proj.ds.Accounts` AS Account KEY (id) PROPERTIES ALL COLUMNS\n)",
      }),
    );
  });

  test("resolves deployments BigQuery URI and rejects leading //", () => {
    const compiled = compile(
      makeModelConfig({
        name: "FinGraph",
        deployments: [
          {
            name: "default",
            target:
              "bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/FinGraph",
          },
        ],
        entities: [
          {
            name: "Account",
            source: "bigquery:proj.ds.Accounts",
            primary_key: ["id"],
          },
        ],
      }),
    );

    expect(asPlainObject(compiled.target)).deep.equals({
      database: "customProj",
      schema: "customDs",
      name: "FinGraph",
    });

    expect(() =>
      build(
        makeModelConfig({
          name: "FinGraph",
          deployments: [
            {
              name: "default",
              target:
                "//bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/FinGraph",
            },
          ],
          entities: [
            {
              name: "Account",
              source: "bigquery:proj.ds.Accounts",
              primary_key: ["id"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'FinGraph': deployment 'target' must match 'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/propertyGraphs/<graph>' (got '//bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/FinGraph').",
    );
  });

  test("propagates top-level tags, disabled, and semantic_model description and ai_context examples", () => {
    const compiled = compile(
      makeModelConfig(
        {
          name: "Tagged",
          description: "Financial graph",
          ai_context: {
            examples: ["Find accounts owned by Alice", "List transfers"],
          },
          entities: [
            {
              name: "Account",
              source: "proj.ds.Accounts",
              primary_key: "id",
            },
          ],
        },
        {
          disabled: true,
          tags: ["nightly", "reporting"],
        },
      ),
    );

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("Tagged"),
        canonicalTarget: graphTarget("Tagged"),
        fileName: "definitions/graph.yaml",
        description: "Financial graph\nExamples: Find accounts owned by Alice, List transfers",
        disabled: true,
        tags: ["nightly", "reporting"],
        nodes: [
          {
            name: "Account",
            dataSource: { database: "proj", schema: "ds", name: "Accounts" },
            keys: ["id"],
          },
        ],
        graphBody:
          "NODE TABLES (\n  `proj.ds.Accounts` AS Account KEY (id) PROPERTIES ALL COLUMNS\n)",
      }),
    );
  });

  test("supports all 3 source string formats (bigquery:p.d.t, p.d.t, and GCP table URI) and rejects leading //", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "A",
            source: "bigquery:p1.d1.T1",
            primary_key: ["id"],
          },
          {
            name: "B",
            source: "p2.d2.T2",
            primary_key: ["id"],
          },
          {
            name: "C",
            source: "bigquery.googleapis.com/projects/p3/datasets/d3/tables/T3",
            primary_key: ["id"],
          },
        ],
      }),
    );

    expect(asPlainObject(compiled.nodes)).deep.equals([
      { name: "A", dataSource: { database: "p1", schema: "d1", name: "T1" }, keys: ["id"] },
      { name: "B", dataSource: { database: "p2", schema: "d2", name: "T2" }, keys: ["id"] },
      { name: "C", dataSource: { database: "p3", schema: "d3", name: "T3" }, keys: ["id"] },
    ]);

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "D",
              source: "//bigquery.googleapis.com/projects/p4/datasets/d4/tables/T4",
              primary_key: ["id"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'D': 'source' URI must match 'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/tables/<table>' (got '//bigquery.googleapis.com/projects/p4/datasets/d4/tables/T4').",
    );
  });

  test("resolves 1:N FK relationship without source by inheriting from entity table and primary_key", () => {
    const compiled = compile(
      makeModelConfig({
        name: "UniGraph",
        entities: [
          {
            name: "Process",
            source: "bigquery:p.d.processes",
            primary_key: ["process_id"],
          },
          {
            name: "Asset",
            source: "bigquery:p.d.assets",
            primary_key: ["hostname"],
          },
        ],
        relationships: [
          {
            name: "ProcessExecutedOn",
            from: "Process",
            to: "Asset",
            from_columns: ["hostname"],
            to_columns: ["hostname"],
          },
        ],
      }),
    );

    expect(asPlainObject(compiled)).deep.equals(
      asPlainObject({
        target: graphTarget("UniGraph"),
        canonicalTarget: graphTarget("UniGraph"),
        fileName: "definitions/graph.yaml",
        description: "",
        disabled: false,
        nodes: [
          {
            name: "Process",
            dataSource: { database: "p", schema: "d", name: "processes" },
            keys: ["process_id"],
          },
          {
            name: "Asset",
            dataSource: { database: "p", schema: "d", name: "assets" },
            keys: ["hostname"],
          },
        ],
        edges: [
          {
            name: "ProcessExecutedOn",
            dataSource: { database: "p", schema: "d", name: "processes" },
            keys: ["process_id"],
            source: {
              node: "Process",
              edgeColumns: ["process_id"],
              nodeColumns: ["process_id"],
            },
            destination: {
              node: "Asset",
              edgeColumns: ["hostname"],
              nodeColumns: ["hostname"],
            },
          },
        ],
        graphBody:
          "NODE TABLES (\n" +
          "  `p.d.processes` AS Process KEY (process_id) PROPERTIES ALL COLUMNS,\n" +
          "  `p.d.assets` AS Asset KEY (hostname) PROPERTIES ALL COLUMNS\n" +
          ")\n" +
          "EDGE TABLES (\n" +
          "  `p.d.processes` AS ProcessExecutedOn KEY (process_id) " +
          "SOURCE KEY (process_id) REFERENCES Process (process_id) " +
          "DESTINATION KEY (hostname) REFERENCES Asset (hostname) NO PROPERTIES\n" +
          ")",
      }),
    );
  });

  test("resolves M:N relationship with explicit source and primary_key and emits NO PROPERTIES", () => {
    const compiled = compile(
      makeModelConfig({
        name: "LibraryGraph",
        entities: [
          {
            name: "Author",
            source: "bigquery:p.d.authors",
            primary_key: ["id"],
          },
          {
            name: "Book",
            source: "bigquery:p.d.books",
            primary_key: ["id"],
          },
        ],
        relationships: [
          {
            name: "Wrote",
            source: "bigquery:p.d.wrote",
            primary_key: ["author_id", "book_id"],
            from: "Author",
            to: "Book",
            from_columns: ["author_id"],
            to_columns: ["book_id"],
            description: "Authorship edge",
            ai_context: {
              synonyms: ["authored"],
              examples: ["Alice wrote Book A"],
            },
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.authors` AS Author KEY (id) PROPERTIES ALL COLUMNS,\n" +
        "  `p.d.books` AS Book KEY (id) PROPERTIES ALL COLUMNS\n" +
        ")\n" +
        "EDGE TABLES (\n" +
        "  `p.d.wrote` AS Wrote KEY (author_id, book_id) " +
        "SOURCE KEY (author_id) REFERENCES Author (id) " +
        "DESTINATION KEY (book_id) REFERENCES Book (id) " +
        'DEFAULT LABEL OPTIONS(description="Authorship edge\\nExamples: Alice wrote Book A", synonyms=["authored"]) ' +
        "NO PROPERTIES\n" +
        ")",
    );
  });

  test("passes derived field expressions as-is into PROPERTIES (...) and emits field OPTIONS", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Person",
            source: "bigquery:p.d.people",
            primary_key: ["id"],
            fields: [
              { name: "id", datatype: "INT64", dimension: false, label: "ID" },
              {
                name: "full_name",
                datatype: "STRING",
                dimension: true,
                label: "Full Name",
                expression: "(first || ' ' || last || ' (Person.literal)')",
                description: "Combined name",
                ai_context: {
                  synonyms: ["display_name"],
                  examples: ["Alice Smith"],
                },
              },
            ],
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.people` AS Person KEY (id) PROPERTIES (" +
        "id, " +
        "(first || ' ' || last || ' (Person.literal)') AS full_name " +
        'OPTIONS(description="Combined name\\nExamples: Alice Smith", synonyms=["display_name"])' +
        ")\n" +
        ")",
    );
  });

  test("emits DEFAULT LABEL OPTIONS(...) before PROPERTIES ALL COLUMNS when entity has ai_context", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Account",
            source: "bigquery:p.d.accounts",
            primary_key: ["id"],
            description: "Bank account",
            ai_context: {
              synonyms: ["acct", "wallet"],
              examples: ["Checking account"],
            },
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.accounts` AS Account KEY (id) " +
        'DEFAULT LABEL OPTIONS(description="Bank account\\nExamples: Checking account", synonyms=["acct", "wallet"]) ' +
        "PROPERTIES ALL COLUMNS\n" +
        ")",
    );
  });

  test("compiles top-level metrics to MEASURE(...) and passes metric expressions as-is", () => {
    const compiled = compile(
      makeModelConfig({
        name: "UniGraph",
        entities: [
          {
            name: "Process",
            source: "bigquery:p.d.uni_processes",
            primary_key: ["process_id"],
            fields: ["process_id", "hostname"],
          },
        ],
        metrics: [
          {
            name: "process_count",
            entity: "Process",
            expression: "COUNT(process_id)",
            description: "Total processes",
            ai_context: {
              synonyms: ["proc_cnt"],
              examples: ["How many processes?"],
            },
          },
          {
            name: "row_count",
            entity: "Process",
            expression: "COUNT(*)",
          },
          {
            name: "distinct_host_count",
            entity: "Process",
            expression: "COUNT(DISTINCT hostname)",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.uni_processes` AS Process KEY (process_id) PROPERTIES (" +
        "process_id, " +
        "hostname, " +
        "MEASURE(COUNT(process_id)) AS process_count " +
        'OPTIONS(description="Total processes\\nExamples: How many processes?", synonyms=["proc_cnt"]), ' +
        "MEASURE(COUNT(*)) AS row_count, " +
        "MEASURE(COUNT(DISTINCT hostname)) AS distinct_host_count" +
        ")\n" +
        ")",
    );
  });

  test("errors when an entity targeted by metrics omits fields", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "bigquery:p.d.uni_processes",
              primary_key: ["process_id"],
            },
          ],
          metrics: [
            {
              name: "process_count",
              entity: "Process",
              expression: "COUNT(process_id)",
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'Process' must declare 'fields' explicitly when 'metrics' target it, because BigQuery does not allow combining PROPERTIES ALL COLUMNS with MEASURE(...).",
    );
  });

  test("matches metric.entity case-insensitively, preserves derived field and metric expressions as-is, and normalizes key backticks", () => {
    const compiled = compile(
      makeModelConfig({
        name: "OrderGraph",
        entities: [
          {
            name: "Order",
            source: "bigquery:p.d.orders",
            primary_key: ["`id`"],
            fields: [
              "id",
              "price",
              "qty",
              "ts",
              {
                name: "total",
                expression: "price * qty",
              },
            ],
          },
        ],
        metrics: [
          {
            name: "sum_total",
            entity: "order",
            expression: "SUM(price * qty)",
          },
          {
            name: "order_count",
            entity: "Order",
            expression: "COUNT(id)",
          },
          {
            name: "sum_year",
            entity: "Order",
            expression: "SUM(EXTRACT(YEAR FROM ts))",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.orders` AS `Order` KEY (`id`) PROPERTIES (" +
        "id, price, qty, ts, price * qty AS total, " +
        "MEASURE(SUM(price * qty)) AS sum_total, " +
        "MEASURE(COUNT(id)) AS order_count, " +
        "MEASURE(SUM(EXTRACT(YEAR FROM ts))) AS sum_year" +
        ")\n" +
        ")",
    );
  });

  test("quotes GoogleSQL reserved keywords in entity, relationship, key, and field identifiers and supports STRUCT field expressions", () => {
    const compiled = compile(
      makeModelConfig({
        name: "ReservedAndStructGraph",
        entities: [
          {
            name: "Person",
            source: "bigquery:p.d.people",
            primary_key: ["id"],
            fields: [
              "id",
              "address",
              {
                name: "city",
                expression: "address.city",
              },
              {
                name: "select",
                expression: "raw_select",
              },
            ],
          },
          {
            name: "Address",
            source: "bigquery:p.d.addresses",
            primary_key: ["id"],
          },
          {
            name: "Order",
            source: "bigquery:p.d.orders",
            primary_key: ["group"],
            unique_keys: [],
            fields: ["group"],
          },
        ],
        relationships: [
          {
            name: "Group",
            from: "order",
            to: "person",
            from_columns: ["person_id"],
            to_columns: ["id"],
          },
        ],
        metrics: [
          {
            name: "city_count",
            entity: "Person",
            expression: "COUNT(Address.city)",
          },
          {
            name: "order_count",
            entity: "Order",
            expression: "COUNT(`group`)",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.people` AS Person KEY (id) PROPERTIES (" +
        "id, address, address.city AS city, raw_select AS `select`, " +
        "MEASURE(COUNT(Address.city)) AS city_count" +
        "),\n" +
        "  `p.d.addresses` AS Address KEY (id) PROPERTIES ALL COLUMNS,\n" +
        "  `p.d.orders` AS `Order` KEY (`group`) PROPERTIES (" +
        "`group`, MEASURE(COUNT(`group`)) AS order_count" +
        ")\n" +
        ")\n" +
        "EDGE TABLES (\n" +
        "  `p.d.orders` AS `Group` KEY (`group`) " +
        "SOURCE KEY (`group`) REFERENCES `Order` (`group`) " +
        "DESTINATION KEY (person_id) REFERENCES Person (id) NO PROPERTIES\n" +
        ")",
    );
  });

  test("defaults field expression to field name when omitted", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Process",
            source: "bigquery:p.d.uni_processes",
            primary_key: ["process_id"],
            fields: [{ name: "process_id" }],
          },
        ],
        metrics: [
          {
            name: "process_count",
            entity: "Process",
            expression: "COUNT(process_id)",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.uni_processes` AS Process KEY (process_id) PROPERTIES (" +
        "process_id, " +
        "MEASURE(COUNT(process_id)) AS process_count" +
        ")\n" +
        ")",
    );
  });

  test("resolves source.ref for entities and 1:N FK edges during finalize() and records dependencyTargets", () => {
    const session = makeSession();
    session.resolveTarget = (ref: dataform.ITarget) =>
      dataform.Target.create({
        database: ref.database || "defaultProject",
        schema: ref.schema || "defaultDataset",
        name: ref.name,
      });

    const action = new PropertyGraph(
      session,
      makeModelConfig({
        name: "RefGraph",
        entities: [
          {
            name: "Process",
            source: { ref: "processes" },
            primary_key: ["process_id"],
          },
          {
            name: "Asset",
            source: {
              ref: { name: "assets", schema: "customDs", include_dependent_assertions: true },
            },
            primary_key: ["hostname"],
          },
        ],
        relationships: [
          {
            name: "ProcessExecutedOn",
            from: "Process",
            to: "Asset",
            from_columns: ["hostname"],
            to_columns: ["hostname"],
          },
        ],
      }),
      "definitions/graph.yaml",
    );

    const pg = action.compile();
    action.finalize();

    expect(asPlainObject(pg.dependencyTargets)).deep.equals([
      { name: "processes", includeDependentAssertions: false },
      { name: "assets", schema: "customDs", includeDependentAssertions: true },
    ]);
    expect(pg.graphBody).equals(
      "NODE TABLES (\n" +
        "  `defaultProject.defaultDataset.processes` AS Process KEY (process_id) PROPERTIES ALL COLUMNS,\n" +
        "  `defaultProject.customDs.assets` AS Asset KEY (hostname) PROPERTIES ALL COLUMNS\n" +
        ")\n" +
        "EDGE TABLES (\n" +
        "  `defaultProject.defaultDataset.processes` AS ProcessExecutedOn KEY (process_id) " +
        "SOURCE KEY (process_id) REFERENCES Process (process_id) " +
        "DESTINATION KEY (hostname) REFERENCES Asset (hostname) NO PROPERTIES\n" +
        ")",
    );
  });

  test("errors when version is missing or invalid", () => {
    expect(() =>
      build({
        semantic_model: [
          {
            name: "G",
            entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
          },
        ],
      }),
    ).to.throw("Property graph config must declare 'version: 0.2.0.dev0/google'.");

    expect(() =>
      build({
        version: "0.1.0",
        semantic_model: [
          {
            name: "G",
            entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
          },
        ],
      }),
    ).to.throw("Property graph config 'version' must be '0.2.0.dev0/google' (got '0.1.0').");
  });

  test("errors when semantic_model is missing, empty, or has more than 1 model in V1", () => {
    expect(() => build({ version: "0.2.0.dev0/google" })).to.throw(
      "Property graph config must declare a non-empty 'semantic_model' list.",
    );

    expect(() =>
      build({
        version: "0.2.0.dev0/google",
        semantic_model: [
          { name: "G1", entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }] },
          { name: "G2", entities: [{ name: "B", source: "p.d.B", primary_key: ["id"] }] },
        ],
      }),
    ).to.throw(
      "Property graph config 'semantic_model' must contain exactly 1 model in V1 (found 2).",
    );
  });

  test("errors when deployment target graph name does not match semantic_model name", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "FinGraph",
          deployments: [
            {
              name: "default",
              target: "bigquery.googleapis.com/projects/p/datasets/d/propertyGraphs/WrongGraphName",
            },
          ],
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw(
      "Property graph 'FinGraph': deployment target graph name 'WrongGraphName' must match semantic_model name 'FinGraph'.",
    );
  });

  test("errors when entity declares unique_keys in V1", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Account",
              source: "p.d.Accounts",
              primary_key: ["id"],
              unique_keys: [["iban"]],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'Account': 'unique_keys' are not supported in V1 because BigQuery CREATE PROPERTY GRAPH DDL only supports a single KEY(...) clause.",
    );
  });

  test("errors when primary_key, from_columns, or to_columns contain non-physical column expressions or qualifiers", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "p.d.processes",
              primary_key: ["Process.process_id"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'Process': 'primary_key' entry 'Process.process_id' must be a physical column name",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Process", source: "p.d.processes", primary_key: ["process_id"] },
            { name: "Asset", source: "p.d.assets", primary_key: ["hostname"] },
          ],
          relationships: [
            {
              name: "Exec",
              from: "Process",
              to: "Asset",
              from_columns: ["LOWER(hostname)"],
              to_columns: ["hostname"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': relationship 'Exec': 'from_columns' entry 'LOWER(hostname)' must be a physical column name",
    );
  });

  test("allows backtick-quoted physical column names in primary_key", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Process",
            source: "p.d.processes",
            primary_key: ["`select`"],
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n  `p.d.processes` AS Process KEY (`select`) PROPERTIES ALL COLUMNS\n)",
    );
  });

  test("enforces case-insensitive uniqueness on entities, relationships, metrics, and fields", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Account", source: "p.d.A", primary_key: ["id"] },
            { name: "account", source: "p.d.B", primary_key: ["id"] },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': duplicate entity name 'account' (conflicts case-insensitively with 'Account').",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Account",
              source: "p.d.A",
              primary_key: ["id"],
              fields: [{ name: "balance" }, { name: "Balance" }],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'Account': duplicate field name 'Balance' (conflicts case-insensitively with 'balance').",
    );
  });

  test("errors when metric is missing entity, references unknown entity, or collides with an entity field", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Process", source: "p.d.P", primary_key: ["id"] }],
          metrics: [{ name: "m1", expression: "COUNT(*)" }],
        }),
      ),
    ).to.throw("Property graph 'G': metric 'm1' must declare 'entity'.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Process", source: "p.d.P", primary_key: ["id"] }],
          metrics: [{ name: "m1", entity: "Unknown", expression: "COUNT(*)" }],
        }),
      ),
    ).to.throw("Property graph 'G': metric 'm1' references unknown entity 'Unknown'.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "p.d.P",
              primary_key: ["id"],
              fields: [{ name: "process_count" }],
            },
          ],
          metrics: [
            {
              name: "PROCESS_COUNT",
              entity: "Process",
              expression: "COUNT(Process.id)",
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': metric 'PROCESS_COUNT' collides with field 'process_count' on entity 'Process'.",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "p.d.P",
              primary_key: ["id"],
              fields: ["id"],
            },
          ],
          metrics: [
            {
              name: "m1",
              entity: "Process",
              expression: "   ",
            },
          ],
        }),
      ),
    ).to.throw("Property graph 'G': metric 'm1' must declare 'expression'.");
  });

  test("defaults 1:N relationship to_columns to target entity primary_key when omitted and rejects mismatched to_columns", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          { name: "Process", source: "p.d.processes", primary_key: ["process_id"] },
          { name: "Machine", source: "p.d.machines", primary_key: ["machine_id"] },
        ],
        relationships: [
          {
            name: "RunsOn",
            from: "Process",
            to: "Machine",
            from_columns: ["machine_fk"],
          },
        ],
      }),
    );

    expect(compiled.graphBody).to.include(
      "SOURCE KEY (process_id) REFERENCES Process (process_id) " +
        "DESTINATION KEY (machine_fk) REFERENCES Machine (machine_id) NO PROPERTIES",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Process", source: "p.d.processes", primary_key: ["process_id"] },
            { name: "Machine", source: "p.d.machines", primary_key: ["machine_id"] },
          ],
          relationships: [
            {
              name: "RunsOn",
              from: "Process",
              to: "Machine",
              from_columns: ["machine_fk"],
              to_columns: ["wrong_col"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': relationship 'RunsOn': in a 1:N relationship (without 'source'), 'to_columns' ([wrong_col]) must match target entity 'Machine' primary_key ([machine_id]).",
    );
  });

  test("errors when M:N relationship with explicit source omits primary_key or to_columns", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Author", source: "p.d.authors", primary_key: ["id"] },
            { name: "Book", source: "p.d.books", primary_key: ["id"] },
          ],
          relationships: [
            {
              name: "Wrote",
              source: "p.d.wrote",
              from: "Author",
              to: "Book",
              from_columns: ["author_id"],
              to_columns: ["book_id"],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': relationship 'Wrote' with 'source' must declare 'primary_key'.",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Author", source: "p.d.authors", primary_key: ["id"] },
            { name: "Book", source: "p.d.books", primary_key: ["id"] },
          ],
          relationships: [
            {
              name: "Wrote",
              source: "p.d.wrote",
              primary_key: ["rel_id"],
              from: "Author",
              to: "Book",
              from_columns: ["author_id"],
            },
          ],
        }),
      ),
    ).to.throw("Property graph 'G': relationship 'Wrote' with 'source' must declare 'to_columns'.");
  });

  test("errors with a migration message when legacy nodes/edges schema is used", () => {
    expect(() =>
      build({
        name: "LegacyGraph",
        nodes: [{ name: "N", source: "p.d.N", keys: ["id"] }],
      }),
    ).to.throw(
      "Legacy property graph schema ('nodes'/'edges') is no longer supported; migrate to Semantic Ontology V1 ('version: 0.2.0.dev0/google' and 'semantic_model').",
    );
  });

  test("errors when entity, relationship, field, or metric name is not a valid identifier", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Bad Name", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity name 'Bad Name' must be a valid identifier (letters, digits, and underscores, starting with a letter or underscore).",
    );
  });

  test("errors when deployments has more than 1 entry or an invalid target URI", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          deployments: [
            {
              name: "d1",
              target: "bigquery.googleapis.com/projects/p/datasets/d/propertyGraphs/G",
            },
            {
              name: "d2",
              target: "bigquery.googleapis.com/projects/p/datasets/d/propertyGraphs/G",
            },
          ],
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw("Property graph 'G': at most one deployment is supported in V1 (found 2).");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          deployments: [{ name: "d1", target: "invalid://uri" }],
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw(
      "Property graph 'G': deployment 'target' must match 'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/propertyGraphs/<graph>' (got 'invalid://uri').",
    );
  });

  test("rejects array source.ref, source object missing ref, or extra properties on source", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "A", source: { ref: ["bad"] }, primary_key: ["id"] }],
        }),
      ),
    ).to.throw("Property graph 'G': entity 'A': 'source.ref' must include a 'name'.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "A",
              source: { name: "accounts", schema: "other_schema" },
              primary_key: ["id"],
            },
          ],
        }),
      ),
    ).to.throw("'source' object must contain a 'ref' property.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "A", source: { ref: "accounts", schema: "other_schema" }, primary_key: ["id"] },
          ],
        }),
      ),
    ).to.throw(
      "Unexpected property 'schema' in 'source' (only 'ref' is allowed; nest target properties inside 'source.ref').",
    );
  });

  test("passes arbitrary SQL expressions (including qualified names, ratios, literals, and backticks) as-is into field and metric DDL", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Account",
            source: "bigquery:p.d.accounts",
            primary_key: ["id"],
            fields: [
              "id",
              {
                name: "dotted_col",
                expression: "`Account.id`",
              },
              {
                name: "sub_country",
                expression: "`sub-account`.country",
              },
              {
                name: "prefixed_col",
                expression: "Account.name",
              },
            ],
          },
          {
            name: "Customer",
            source: "bigquery:p.d.customers",
            primary_key: ["id"],
            fields: ["id"],
          },
        ],
        metrics: [
          {
            name: "cnt_one",
            entity: "Customer",
            expression: "COUNT(1)",
          },
          {
            name: "sub_account_cnt",
            entity: "Customer",
            expression: "COUNT(`sub-account`.country)",
          },
          {
            name: "avg_ratio",
            entity: "Customer",
            expression: "SUM(x) / COUNT(y)",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.accounts` AS Account KEY (id) PROPERTIES (" +
        "id, `Account.id` AS dotted_col, `sub-account`.country AS sub_country, Account.name AS prefixed_col" +
        "),\n" +
        "  `p.d.customers` AS Customer KEY (id) PROPERTIES (" +
        "id, MEASURE(COUNT(1)) AS cnt_one, MEASURE(COUNT(`sub-account`.country)) AS sub_account_cnt, MEASURE(SUM(x) / COUNT(y)) AS avg_ratio" +
        ")\n" +
        ")",
    );
  });
});
