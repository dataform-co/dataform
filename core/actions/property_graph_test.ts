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
      "Property graph 'FinGraph': deployment 'target' must not start with '//'; remove the leading '//' and use 'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/propertyGraphs/<graph>' (got '//bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/FinGraph').",
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
        description: "Financial graph\n\nExamples: Find accounts owned by Alice; List transfers",
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
      "Property graph 'G': entity 'D': 'source' URI must not start with '//'; remove the leading '//' and use 'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/tables/<table>' (got '//bigquery.googleapis.com/projects/p4/datasets/d4/tables/T4').",
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

  test("emits relationship DEFAULT LABEL OPTIONS(...) and NO PROPERTIES", () => {
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
            name: "WrittenBy",
            from: "Book",
            to: "Author",
            from_columns: ["author_id"],
            to_columns: ["id"],
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
        "  `p.d.books` AS WrittenBy KEY (id) " +
        "SOURCE KEY (id) REFERENCES Book (id) " +
        "DESTINATION KEY (author_id) REFERENCES Author (id) " +
        'DEFAULT LABEL OPTIONS(description="Authorship edge", synonyms=["authored"], extensions=[("examples", "[\\"Alice wrote Book A\\"]")]) ' +
        "NO PROPERTIES\n" +
        ")",
    );
  });

  test("passes derived field expressions as-is into PROPERTIES (...), folds label and dimension.is_time into description, and emits instructions and examples in extensions", () => {
    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Person",
            source: "bigquery:p.d.people",
            primary_key: ["id"],
            fields: [
              { name: "id", datatype: "INT64" },
              {
                name: "full_name",
                datatype: "STRING",
                label: "Full Name",
                expression: "(first || ' ' || last || ' (Person.literal)')",
                description: "Combined name",
                ai_context: {
                  instructions: "Prefer full_name in display output.",
                  synonyms: ["display_name"],
                  examples: ["Alice Smith", "Bob Jones"],
                },
              },
              {
                name: "started_at",
                dimension: { is_time: true },
                ai_context: {
                  instructions: "Use UTC for comparisons.",
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
        'OPTIONS(description="Full Name\\n\\nCombined name", synonyms=["display_name"], extensions=[("instructions", "Prefer full_name in display output."), ("examples", "[\\"Alice Smith\\",\\"Bob Jones\\"]")]), ' +
        "started_at " +
        'OPTIONS(description="Time dimension.", extensions=[("instructions", "Use UTC for comparisons.")])' +
        ")\n" +
        ")",
    );
  });

  test("emits DEFAULT LABEL OPTIONS(...) before PROPERTIES ALL COLUMNS when entity has ai_context (including extensions-only)", () => {
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
          {
            name: "AuditLog",
            source: "bigquery:p.d.audit_logs",
            primary_key: ["log_id"],
            ai_context: "Filter by recent timestamps.",
          },
        ],
        relationships: [
          {
            name: "LoggedFor",
            from: "AuditLog",
            to: "Account",
            from_columns: ["account_id"],
            to_columns: ["id"],
            ai_context: {
              instructions: "Join audit entries to their owning account.",
            },
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.accounts` AS Account KEY (id) " +
        'DEFAULT LABEL OPTIONS(description="Bank account", synonyms=["acct", "wallet"], extensions=[("examples", "[\\"Checking account\\"]")]) ' +
        "PROPERTIES ALL COLUMNS,\n" +
        "  `p.d.audit_logs` AS AuditLog KEY (log_id) " +
        'DEFAULT LABEL OPTIONS(extensions=[("instructions", "Filter by recent timestamps.")]) ' +
        "PROPERTIES ALL COLUMNS\n" +
        ")\n" +
        "EDGE TABLES (\n" +
        "  `p.d.audit_logs` AS LoggedFor KEY (log_id) " +
        "SOURCE KEY (log_id) REFERENCES AuditLog (log_id) " +
        "DESTINATION KEY (account_id) REFERENCES Account (id) " +
        'DEFAULT LABEL OPTIONS(extensions=[("instructions", "Join audit entries to their owning account.")]) ' +
        "NO PROPERTIES\n" +
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
              instructions: "Count active OS processes.",
              synonyms: ["proc_cnt"],
              examples: ["How many processes?"],
            },
          },
          {
            name: "named_host_count",
            entity: "Process",
            expression: "COUNT(hostname)",
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
        'OPTIONS(description="Total processes", synonyms=["proc_cnt"], extensions=[("instructions", "Count active OS processes."), ("examples", "[\\"How many processes?\\"]")]), ' +
        "MEASURE(COUNT(hostname)) AS named_host_count, " +
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

  test("allows backtick-quoted valid physical column names in primary_key and rejects backtick-quoted non-identifiers", () => {
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

    for (const badKey of ["`a.b`", "`x y`"]) {
      expect(() =>
        build(
          makeModelConfig({
            name: "G",
            entities: [
              {
                name: "Process",
                source: "p.d.processes",
                primary_key: [badKey],
              },
            ],
          }),
        ),
      ).to.throw(
        `Property graph 'G': entity 'Process': 'primary_key' entry '${badKey}' must be a physical column name`,
      );
    }
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
          metrics: [{ name: "m1", expression: "COUNT(id)" }],
        }),
      ),
    ).to.throw("Property graph 'G': metric 'm1' must declare 'entity'.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Process", source: "p.d.P", primary_key: ["id"] }],
          metrics: [{ name: "m1", entity: "Unknown", expression: "COUNT(id)" }],
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

  test("requires explicit relationship to_columns and rejects mismatched to_columns", () => {
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
            },
          ],
        }),
      ),
    ).to.throw("Property graph 'G': relationship 'RunsOn' must declare 'to_columns'.");

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
      "Property graph 'G': relationship 'RunsOn': 'to_columns' ([wrong_col]) must match target entity 'Machine' primary_key ([machine_id]).",
    );
  });

  test("errors when relationship declares source or primary_key in V1", () => {
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
              primary_key: ["author_id", "book_id"],
              from: "Author",
              to: "Book",
              from_columns: ["author_id"],
              to_columns: ["book_id"],
            },
          ],
        }),
      ),
    ).to.throw('Unexpected property "source", or property value type of "string" is incorrect.');
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
            name: "cnt_positive",
            entity: "Customer",
            expression: "COUNT(CASE WHEN id > 0 THEN 1 END)",
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
        "id, MEASURE(COUNT(CASE WHEN id > 0 THEN 1 END)) AS cnt_positive, MEASURE(COUNT(`sub-account`.country)) AS sub_account_cnt, MEASURE(SUM(x) / COUNT(y)) AS avg_ratio" +
        ")\n" +
        ")",
    );
  });

  test("rejects COUNT(*) and literal-only COUNT(...) in metrics while allowing valid property, string-literal, and comment cases", () => {
    for (const starExpr of ["COUNT(*)", "count( * )", "SUM(x) / NULLIF(COUNT(*), 0)"]) {
      expect(() =>
        build(
          makeModelConfig({
            name: "G",
            entities: [
              { name: "Order", source: "p.d.orders", primary_key: ["id"], fields: ["id"] },
            ],
            metrics: [{ name: "m_star", entity: "Order", expression: starExpr }],
          }),
        ),
      ).to.throw(
        "Property graph 'G': metric 'm_star': 'COUNT(*)' is not supported in BigQuery property graph measure expressions; count an explicit property instead (for example, 'COUNT(id)').",
      );
    }

    for (const [literalExpr, shownArg] of [
      ["COUNT(1)", "1"],
      ["COUNT(0)", "0"],
      ["COUNT(-1)", "-1"],
      ["COUNT(3.14)", "3.14"],
      ["COUNT((1))", "(1)"],
      ["COUNT(DISTINCT 1)", "DISTINCT 1"],
      ["COUNT(ALL 1)", "ALL 1"],
      ["COUNT('x')", "'x'"],
      ['COUNT("x")', '"x"'],
      ["COUNT(r'x')", "r'x'"],
      ["COUNT(TRUE)", "TRUE"],
      ["COUNT(NULL)", "NULL"],
    ] as const) {
      expect(() =>
        build(
          makeModelConfig({
            name: "G",
            entities: [
              { name: "Order", source: "p.d.orders", primary_key: ["id"], fields: ["id"] },
            ],
            metrics: [{ name: "m_lit", entity: "Order", expression: literalExpr }],
          }),
        ),
      ).to.throw(
        `Property graph 'G': metric 'm_lit': 'COUNT(${shownArg})' is not supported in BigQuery property graph measure expressions because a measure must aggregate at least one graph property; count an explicit property instead (for example, 'COUNT(id)').`,
      );
    }

    const compiled = compile(
      makeModelConfig({
        name: "G",
        entities: [
          {
            name: "Order",
            source: "p.d.orders",
            primary_key: ["id"],
            fields: ["id", "status"],
          },
        ],
        metrics: [
          {
            name: "quoted_star_in_string",
            entity: "Order",
            expression: "COUNT(CASE WHEN status = 'COUNT(*)' THEN id END)",
          },
          {
            name: "commented_star",
            entity: "Order",
            expression: "/* COUNT(*) */ COUNT(id) -- COUNT(1)",
          },
          {
            name: "backticked_digit_col",
            entity: "Order",
            expression: "COUNT(`1`)",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.orders` AS `Order` KEY (id) PROPERTIES (" +
        "id, status, " +
        "MEASURE(COUNT(CASE WHEN status = 'COUNT(*)' THEN id END)) AS quoted_star_in_string, " +
        "MEASURE(/* COUNT(*) */ COUNT(id) -- COUNT(1)) AS commented_star, " +
        "MEASURE(COUNT(`1`)) AS backticked_digit_col" +
        ")\n" +
        ")",
    );
  });

  test("supports derived AGG(...) sibling metrics and optional datatype on metrics", () => {
    const compiled = compile(
      makeModelConfig({
        name: "CourseGraph",
        entities: [
          {
            name: "Course",
            source: "bigquery:p.d.courses",
            primary_key: ["course_id"],
            fields: ["course_id", "credits", "dropped_credits"],
          },
        ],
        metrics: [
          {
            name: "total_credits",
            entity: "Course",
            datatype: "Integer",
            expression: "SUM(credits)",
          },
          {
            name: "total_dropped_credits",
            entity: "Course",
            datatype: "INT64",
            expression: "SUM(dropped_credits)",
          },
          {
            name: "course_count",
            entity: "Course",
            datatype: "Integer",
            expression: "COUNT(course_id)",
          },
          {
            name: "net_credits",
            entity: "Course",
            datatype: "Integer",
            expression: "AGG(total_credits) - AGG(total_dropped_credits)",
          },
          {
            name: "avg_credits",
            entity: "Course",
            datatype: "FLOAT64",
            expression: "SAFE_DIVIDE(AGG(total_credits), NULLIF(AGG(course_count), 0))",
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        "  `p.d.courses` AS Course KEY (course_id) PROPERTIES (" +
        "course_id, credits, dropped_credits, " +
        "MEASURE(SUM(credits)) AS total_credits, " +
        "MEASURE(SUM(dropped_credits)) AS total_dropped_credits, " +
        "MEASURE(COUNT(course_id)) AS course_count, " +
        "MEASURE(AGG(total_credits) - AGG(total_dropped_credits)) AS net_credits, " +
        "MEASURE(SAFE_DIVIDE(AGG(total_credits), NULLIF(AGG(course_count), 0))) AS avg_credits" +
        ")\n" +
        ")",
    );
  });

  test("accepts ai_context.custom and non-GOOGLE custom_extensions on all levels, preserves nested snake_case keys, and omits them from BigQuery DDL", () => {
    const rawConfig = makeModelConfig({
      name: "KcGraph",
      description: "Model desc",
      ai_context: {
        instructions: "Model instructions",
        custom: {
          owner_team: "analytics",
          nested_meta: {
            snake_key: 42,
            is_certified: false,
            nullable_val: null,
            tag_list: ["a", { item_key: "b" }],
            fields: ["not_a_proto_struct_fields_map"],
            empty_obj: {},
            empty_arr: [],
          },
        },
      },
      custom_extensions: [
        { vendor_name: "dbt", data: '{"meta":{"model_tier":"gold"}}' },
        { vendor_name: "looker", data: { view_label: "Core", snake_flag: true } },
      ],
      entities: [
        {
          name: "Process",
          source: "bigquery:p.d.processes",
          primary_key: ["process_id"],
          ai_context: {
            synonyms: ["proc"],
            custom: { domain_owner: "infra" },
          },
          custom_extensions: [{ vendor_name: "dbt", data: '{"entity":true}' }],
          fields: [
            {
              name: "process_id",
              ai_context: { custom: { pii_level: "none" } },
              custom_extensions: [{ vendor_name: "dbt", data: '{"pk":true}' }],
            },
            { name: "machine_id" },
            { name: "cpu" },
          ],
        },
        {
          name: "Machine",
          source: "bigquery:p.d.machines",
          primary_key: ["machine_id"],
        },
      ],
      relationships: [
        {
          name: "RunsOn",
          from: "Process",
          to: "Machine",
          from_columns: ["machine_id"],
          to_columns: ["machine_id"],
          ai_context: { custom: { cardinality_hint: "many_to_one" } },
          custom_extensions: [{ vendor_name: "dbt", data: '{"edge":true}' }],
        },
      ],
      metrics: [
        {
          name: "total_cpu",
          entity: "Process",
          datatype: "Float",
          expression: "SUM(cpu)",
          ai_context: {
            synonyms: ["cpu_sum"],
            custom: { unit_name: "cores" },
          },
          custom_extensions: [{ vendor_name: "dbt", data: '{"tier":"gold"}' }],
        },
      ],
    });

    const compiled = compile(rawConfig);
    expect(compiled.description).equals("Model desc\n\nModel instructions");
    expect(compiled.graphBody).equals(
      "NODE TABLES (\n" +
        '  `p.d.processes` AS Process KEY (process_id) DEFAULT LABEL OPTIONS(synonyms=["proc"]) ' +
        'PROPERTIES (process_id, machine_id, cpu, MEASURE(SUM(cpu)) AS total_cpu OPTIONS(synonyms=["cpu_sum"])),\n' +
        "  `p.d.machines` AS Machine KEY (machine_id) PROPERTIES ALL COLUMNS\n" +
        ")\n" +
        "EDGE TABLES (\n" +
        "  `p.d.processes` AS RunsOn KEY (process_id) " +
        "SOURCE KEY (process_id) REFERENCES Process (process_id) " +
        "DESTINATION KEY (machine_id) REFERENCES Machine (machine_id) NO PROPERTIES\n" +
        ")",
    );
  });

  test("rejects invalid ai_context.custom shapes and GOOGLE or malformed custom_extensions", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          ai_context: { custom: ["not_an_object"] },
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw("Property graph 'G': 'ai_context.custom' must be a key-value mapping object.");

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "A",
              source: "p.d.A",
              primary_key: ["id"],
              ai_context: { custom: { synonyms: ["misplaced"] } },
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'A': 'ai_context.custom' cannot hold 'synonyms'; write 'synonyms' directly under 'ai_context'.",
    );

    for (const googleSpelling of ["GOOGLE", "google", "Google"]) {
      expect(() =>
        build(
          makeModelConfig({
            name: "G",
            entities: [
              {
                name: "A",
                source: "p.d.A",
                primary_key: ["id"],
                fields: ["id"],
              },
            ],
            metrics: [
              {
                name: "m1",
                entity: "A",
                expression: "COUNT(id)",
                custom_extensions: [{ vendor_name: googleSpelling, data: "{}" }],
              },
            ],
          }),
        ),
      ).to.throw(
        "Property graph 'G': metric 'm1': 'GOOGLE' custom_extensions blocks are not accepted in '0.2.0.dev0/google' documents; declare Google extensions using native keys.",
      );
    }

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          custom_extensions: [{ vendor_name: "  ", data: "{}" }],
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw(
      "Property graph 'G': every 'custom_extensions' entry must declare a non-empty 'vendor_name'.",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          custom_extensions: [{ vendor_name: "dbt", data: ["bad"] }],
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
        }),
      ),
    ).to.throw(
      "Property graph 'G': 'custom_extensions' entry for vendor 'dbt' must declare 'data' as a JSON string or mapping object.",
    );
  });

  test("accepts datasets as an alias for entities and rejects specifying both", () => {
    const compiled = compile(
      makeModelConfig({
        name: "DatasetAliasGraph",
        datasets: [
          {
            name: "Process",
            source: "bigquery:p.d.processes",
            primary_key: ["process_id"],
            fields: ["process_id"],
          },
        ],
      }),
    );

    expect(compiled.graphBody).equals(
      "NODE TABLES (\n  `p.d.processes` AS Process KEY (process_id) PROPERTIES (process_id)\n)",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "BothGraph",
          entities: [{ name: "A", source: "p.d.A", primary_key: ["id"] }],
          datasets: [{ name: "B", source: "p.d.B", primary_key: ["id"] }],
        }),
      ),
    ).to.throw("Cannot specify both 'entities' and 'datasets' in the same semantic_model.");
  });

  test("errors when field or metric expression is a multi-dialect object in V1", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "p.d.processes",
              primary_key: ["process_id"],
              fields: [
                {
                  name: "process_name",
                  expression: {
                    dialects: [{ dialect: "BIGQUERY", expression: "LOWER(name)" }],
                  },
                },
              ],
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': entity 'Process': field 'process_name': 'expression' must be a string; multi-dialect 'dialects' objects are not supported in V1.",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            {
              name: "Process",
              source: "p.d.processes",
              primary_key: ["process_id"],
              fields: [{ name: "process_id" }],
            },
          ],
          metrics: [
            {
              name: "process_count",
              entity: "Process",
              expression: {
                dialects: [{ dialect: "BIGQUERY", expression: "COUNT(process_id)" }],
              },
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': metric 'process_count': 'expression' must be a string; multi-dialect 'dialects' objects are not supported in V1.",
    );
  });
});
