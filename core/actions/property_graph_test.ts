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

  test("resolves deployments BigQuery URI (without and with leading //)", () => {
    for (const prefix of ["", "//"]) {
      const compiled = compile(
        makeModelConfig({
          name: "FinGraph",
          deployments: [
            {
              name: "default",
              target: `${prefix}bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/FinGraph`,
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
    }
  });

  test("propagates tags, disabled, description, and ai_context examples on semantic_model", () => {
    const compiled = compile(
      makeModelConfig({
        name: "Tagged",
        description: "Financial graph",
        ai_context: {
          examples: ["Find accounts owned by Alice", "List transfers"],
        },
        disabled: true,
        tags: ["nightly", "reporting"],
        entities: [
          {
            name: "Account",
            source: "proj.ds.Accounts",
            primary_key: "id",
          },
        ],
      }),
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

  test("supports all 3 source string formats (bigquery:p.d.t, p.d.t, and GCP table URI)", () => {
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
          {
            name: "D",
            source: "//bigquery.googleapis.com/projects/p4/datasets/d4/tables/T4",
            primary_key: ["id"],
          },
        ],
      }),
    );

    expect(asPlainObject(compiled.nodes)).deep.equals([
      { name: "A", dataSource: { database: "p1", schema: "d1", name: "T1" }, keys: ["id"] },
      { name: "B", dataSource: { database: "p2", schema: "d2", name: "T2" }, keys: ["id"] },
      { name: "C", dataSource: { database: "p3", schema: "d3", name: "T3" }, keys: ["id"] },
      { name: "D", dataSource: { database: "p4", schema: "d4", name: "T4" }, keys: ["id"] },
    ]);
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

  test("strips leading <Entity>. and `<Entity>`. qualifiers from derived field expressions outside string literals", () => {
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
                expression: "(Person.first || ' ' || `Person`.last || ' (Person.literal)')",
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

  test("compiles top-level metrics to MEASURE(...), strips <Entity>., lowers COUNT(*), and auto-exposes operand columns", () => {
    const compiled = compile(
      makeModelConfig({
        name: "UniGraph",
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
            expression: "COUNT(Process.process_id)",
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
            expression: "COUNT(DISTINCT Process.hostname)",
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
        "MEASURE(COUNT(process_id)) AS row_count, " +
        "MEASURE(COUNT(DISTINCT hostname)) AS distinct_host_count" +
        ")\n" +
        ")",
    );
  });

  test("does not duplicate operand column when already declared in entity fields", () => {
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
            expression: "COUNT(Process.process_id)",
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
  });

  test("errors when metric expression is not a single supported aggregate", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Process", source: "p.d.P", primary_key: ["id"] }],
          metrics: [
            {
              name: "bad_agg",
              entity: "Process",
              expression: "SUM(Process.x) / COUNT(Process.y)",
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': metric 'bad_agg': expression 'SUM(Process.x) / COUNT(Process.y)' must be a single supported aggregate (SUM, AVG, COUNT, MIN, MAX) over one operand or COUNT(*).",
    );

    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [{ name: "Process", source: "p.d.P", primary_key: ["id"] }],
          metrics: [
            {
              name: "bad_star",
              entity: "Process",
              expression: "SUM(*)",
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': metric 'bad_star': 'SUM(*)' is not supported (only COUNT(*) is allowed).",
    );
  });

  test("defaults 1:N relationship to_columns to target entity primary_key when omitted", () => {
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

  test("errors when metric expression references another declared entity", () => {
    expect(() =>
      build(
        makeModelConfig({
          name: "G",
          entities: [
            { name: "Process", source: "p.d.processes", primary_key: ["id"] },
            { name: "Machine", source: "p.d.machines", primary_key: ["id"] },
          ],
          metrics: [
            {
              name: "bad_metric",
              entity: "Process",
              expression: "COUNT(Machine.id)",
            },
          ],
        }),
      ),
    ).to.throw(
      "Property graph 'G': metric 'bad_metric' targets entity 'Process' but its expression references entity 'Machine'.",
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
});
