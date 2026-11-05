import { expect } from "chai";
import * as fs from "fs-extra";

import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import {
  asPlainObject,
  suite,
  test,
  writeDefinitionFile,
  writeWorkflowSettingsFile,
} from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

interface TestCase {
  testName: string;
  workflowSettings: string;
  definitionFiles: Array<{
    name: string;
    contents: string;
  }>;
  expectedGraph?: dataform.ICompiledGraph;
  expectedPropertyGraphs?: dataform.IPropertyGraph[];
}

suite("property graphs", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const graphProjectConfig = {
    warehouse: "bigquery",
    defaultSchema: "defaultDataset",
    defaultDatabase: "defaultProject",
    defaultLocation: "US",
  };
  const graphError = (fileName: string, message: string, extra: object = {}) => ({
    fileName,
    message,
    ...extra,
  });
  const asPlainGraph = (graph: dataform.ICompiledGraph) => {
    const plain = asPlainObject(graph);
    plain.graphErrors?.compilationErrors?.forEach((e: any) => {
      expect(e.stack).to.include(`Error: ${e.message}`);
      expect(e.stack).to.match(/\n\s+at /);
      delete e.stack;
    });
    return plain;
  };

  const missingRefTarget = {
    schema: "defaultDataset",
    name: "MissingRefGraph",
    database: "defaultProject",
  };
  const declOneTarget = { schema: "one", name: "books", database: "defaultProject" };
  const declTwoTarget = { schema: "two", name: "books", database: "defaultProject" };
  const graphTarget = {
    schema: "defaultDataset",
    name: "AmbiguousRefGraph",
    database: "defaultProject",
  };
  const collisionTarget = {
    schema: "defaultDataset",
    name: "CollisionName",
    database: "defaultProject",
  };
  const collisionActionName = "defaultProject.defaultDataset.CollisionName";
  const collisionTargetJson = `{"schema":"defaultDataset","name":"CollisionName","database":"defaultProject"}`;
  const duplicateActionMessage =
    "Duplicate action name detected. Names within a schema must be unique " +
    "across tables, declarations, assertions, and operations:\n" +
    `"${collisionTargetJson}"`;
  const duplicateCanonicalMessage =
    "Duplicate canonical target detected. Canonical targets must be unique " +
    "across tables, declarations, assertions, and operations:\n" +
    `"${collisionTargetJson}"`;

  const testCases: TestCase[] = [
    {
      testName: "valid graph.yaml compiles end-to-end",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: SimpleGraph
  entities:
  - name: Customer
    source: bigquery:defaultProject.defaultDataset.customers
    primary_key:
    - id
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "defaultDataset", name: "SimpleGraph", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "SimpleGraph",
              database: "defaultProject",
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "SimpleGraph",
              database: "defaultProject",
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            nodes: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject",
                },
                keys: ["id"],
              },
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id) PROPERTIES ALL COLUMNS\n" +
              ")",
          },
        ],
      },
    },
    {
      testName: "graph.yaml tags propagate into the compiled proto",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
tags:
- nightly
- reporting
semantic_model:
- name: TaggedGraph
  entities:
  - name: Customer
    source: bigquery:defaultProject.defaultDataset.customers
    primary_key:
    - id
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "defaultDataset", name: "TaggedGraph", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "TaggedGraph",
              database: "defaultProject",
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "TaggedGraph",
              database: "defaultProject",
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            tags: ["nightly", "reporting"],
            nodes: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject",
                },
                keys: ["id"],
              },
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id) PROPERTIES ALL COLUMNS\n" +
              ")",
          },
        ],
      },
    },
    {
      testName: "empty graph.yaml produces a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: "",
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. " +
                "Expected a top-level object with 'version' and 'semantic_model'.",
            ),
          ],
        },
        dataformCoreVersion: version,
        jitData: {},
      },
    },
    {
      testName: "graph.yaml with only a comment produces a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: "# nothing here yet\n",
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. " +
                "Expected a top-level object with 'version' and 'semantic_model'.",
            ),
          ],
        },
        dataformCoreVersion: version,
        jitData: {},
      },
    },
    {
      testName: "graph.yaml with a top-level scalar produces a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: "just a string\n",
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. " +
                "Expected a top-level object with 'version' and 'semantic_model'.",
            ),
          ],
        },
        dataformCoreVersion: version,
        jitData: {},
      },
    },
    {
      testName: "graph.yaml missing entities produces a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: EmptyGraph
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph 'EmptyGraph' must declare at least one entity.",
            ),
          ],
        },
        dataformCoreVersion: version,
        jitData: {},
      },
    },
    {
      testName: "entity source.ref resolves to a sibling sqlx table and records a dependency",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "customers.sqlx",
          contents: `config { type: "table" }\nSELECT 1 AS id`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: RefGraph
  entities:
  - name: Customer
    source:
      ref: customers
    primary_key:
    - id
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "defaultDataset",
            name: "RefGraph",
            database: "defaultProject",
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RefGraph",
            database: "defaultProject",
          },
          dependencyTargets: [
            {
              schema: "defaultDataset",
              name: "customers",
              database: "defaultProject",
            },
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          nodes: [
            {
              name: "Customer",
              dataSource: {
                schema: "defaultDataset",
                name: "customers",
                database: "defaultProject",
              },
              keys: ["id"],
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.customers` AS Customer KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")",
        },
      ],
    },
    {
      testName: "downstream sqlx model can reference the property graph via ref()",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: UniGraph
  entities:
  - name: Process
    source: bigquery:defaultProject.defaultDataset.processes
    primary_key: [process_id]
`,
        },
        {
          name: "query_graph.sqlx",
          contents:
            'config { type: "table" }\nSELECT * FROM GRAPH_TABLE(${ref("UniGraph")} MATCH (p:Process) COLUMNS (p.process_id))',
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [
          { schema: "defaultDataset", name: "UniGraph", database: "defaultProject" },
          { schema: "defaultDataset", name: "query_graph", database: "defaultProject" },
        ],
        jitData: {},
        tables: [
          dataform.Table.create({
            type: "table",
            target: { schema: "defaultDataset", name: "query_graph", database: "defaultProject" },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "query_graph",
              database: "defaultProject",
            },
            dependencyTargets: [
              { schema: "defaultDataset", name: "UniGraph", database: "defaultProject" },
            ],
            query:
              "\nSELECT * FROM GRAPH_TABLE(`defaultProject.defaultDataset.UniGraph` MATCH (p:Process) COLUMNS (p.process_id))",
            disabled: false,
            fileName: "definitions/query_graph.sqlx",
            enumType: dataform.TableType.TABLE,
            hermeticity: dataform.ActionHermeticity.NON_HERMETIC,
          }),
        ],
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "UniGraph",
              database: "defaultProject",
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "UniGraph",
              database: "defaultProject",
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            nodes: [
              {
                name: "Process",
                dataSource: {
                  schema: "defaultDataset",
                  name: "processes",
                  database: "defaultProject",
                },
                keys: ["process_id"],
              },
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.processes` AS Process KEY (process_id) PROPERTIES ALL COLUMNS\n" +
              ")",
          },
        ],
      },
    },
    {
      testName: "entity source.ref pointing to a non-existent action records a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: MissingRefGraph
  entities:
  - name: Ghost
    source:
      ref: does_not_exist
    primary_key:
    - id
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              'Missing dependency detected: Action "defaultProject.defaultDataset.MissingRefGraph" depends on "{"name":"does_not_exist","includeDependentAssertions":false}" which does not exist',
              {
                actionName: "defaultProject.defaultDataset.MissingRefGraph",
                actionTarget: missingRefTarget,
              },
            ),
          ],
        },
        dataformCoreVersion: version,
        targets: [missingRefTarget],
        jitData: {},
        propertyGraphs: [
          {
            target: missingRefTarget,
            canonicalTarget: missingRefTarget,
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            nodes: [
              {
                name: "Ghost",
                keys: ["id"],
              },
            ],
          },
        ],
      },
    },
    {
      testName: "entity source.ref with an ambiguous action name records a compilation error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "decls.js",
          contents: `
declare({ schema: "one", name: "books" });
declare({ schema: "two", name: "books" });
`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: AmbiguousRefGraph
  entities:
  - name: Book
    source:
      ref: books
    primary_key:
    - id
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              'Ambiguous Action name: {"name":"books","includeDependentAssertions":false}. Did you mean one of: one.books, two.books.',
              {
                actionName: "defaultProject.defaultDataset.AmbiguousRefGraph",
                actionTarget: graphTarget,
              },
            ),
          ],
        },
        dataformCoreVersion: version,
        targets: [graphTarget, declOneTarget, declTwoTarget],
        jitData: {},
        declarations: [
          {
            target: declOneTarget,
            canonicalTarget: declOneTarget,
            fileName: "definitions/decls.js",
          },
          {
            target: declTwoTarget,
            canonicalTarget: declTwoTarget,
            fileName: "definitions/decls.js",
          },
        ],
        propertyGraphs: [
          {
            target: graphTarget,
            canonicalTarget: graphTarget,
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            nodes: [
              {
                name: "Book",
                keys: ["id"],
              },
            ],
          },
        ],
      },
    },
    {
      testName: "property graph colliding with a table target records a duplicate-target error",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "CollisionName.sqlx",
          contents: `config { type: "table" }\nSELECT 1 AS id`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: CollisionName
  entities:
  - name: Book
    source: bigquery:defaultProject.defaultDataset.books
    primary_key:
    - id
`,
        },
      ],
      expectedGraph: {
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError("definitions/CollisionName.sqlx", duplicateActionMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget,
            }),
            graphError("definitions/CollisionName.sqlx", duplicateCanonicalMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget,
            }),
            graphError("definitions/graph.yaml", duplicateActionMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget,
            }),
            graphError("definitions/graph.yaml", duplicateCanonicalMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget,
            }),
          ],
        },
        dataformCoreVersion: version,
        targets: [collisionTarget, collisionTarget],
        jitData: {},
      },
    },
    {
      testName:
        "graph.yaml accepts Semantic Ontology V1 snake_case keys, deployments, 1:N and M:N relationships, and metrics",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "books.sqlx",
          contents: `config { type: "table", assertions: { nonNull: ["id"] } }\nSELECT 1 AS id, 10 AS publisher_id`,
        },
        {
          name: "authors.sqlx",
          contents: `config { type: "table", assertions: { nonNull: ["id"] } }\nSELECT 1 AS id`,
        },
        {
          name: "wrote.sqlx",
          contents: `config { type: "table" }\nSELECT 1 AS author_id, 1 AS book_id`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
depend_on_dependency_assertions: false
semantic_model:
- name: SnakeGraph
  deployments:
  - name: default
    target: //bigquery.googleapis.com/projects/customProj/datasets/customDs/propertyGraphs/SnakeGraph
  entities:
  - name: Book
    source:
      ref:
        name: books
        include_dependent_assertions: true
    primary_key:
    - id
    fields:
    - name: id
      datatype: INT64
  - name: Author
    source:
      ref: authors
    primary_key:
    - id
  relationships:
  - name: Wrote
    source:
      ref: wrote
    primary_key: [author_id, book_id]
    from: Author
    to: Book
    from_columns: [author_id]
    to_columns: [book_id]
  - name: WrittenByPrimaryAuthor
    from: Book
    to: Author
    from_columns: [publisher_id]
    to_columns: [id]
  metrics:
  - name: book_count
    entity: Book
    expression: COUNT(id)
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "customDs",
            name: "SnakeGraph",
            database: "customProj",
          },
          canonicalTarget: {
            schema: "customDs",
            name: "SnakeGraph",
            database: "customProj",
          },
          dependencyTargets: [
            {
              schema: "defaultDataset",
              name: "books",
              database: "defaultProject",
            },
            {
              schema: "defaultDataset",
              name: "defaultDataset_books_assertions_rowConditions",
              database: "defaultProject",
            },
            {
              schema: "defaultDataset",
              name: "authors",
              database: "defaultProject",
            },
            {
              schema: "defaultDataset",
              name: "wrote",
              database: "defaultProject",
            },
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          nodes: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject",
              },
              keys: ["id"],
              labels: [
                {
                  name: "Book",
                  fields: [
                    { name: "id", expression: "id", description: "" },
                    { name: "book_count", expression: "MEASURE(COUNT(id))", description: "" },
                  ],
                  importAll: false,
                  isDefault: true,
                },
              ],
            },
            {
              name: "Author",
              dataSource: {
                schema: "defaultDataset",
                name: "authors",
                database: "defaultProject",
              },
              keys: ["id"],
            },
          ],
          edges: [
            {
              name: "Wrote",
              dataSource: {
                schema: "defaultDataset",
                name: "wrote",
                database: "defaultProject",
              },
              keys: ["author_id", "book_id"],
              source: {
                node: "Author",
                edgeColumns: ["author_id"],
                nodeColumns: ["id"],
              },
              destination: {
                node: "Book",
                edgeColumns: ["book_id"],
                nodeColumns: ["id"],
              },
            },
            {
              name: "WrittenByPrimaryAuthor",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject",
              },
              keys: ["id"],
              source: {
                node: "Book",
                edgeColumns: ["id"],
                nodeColumns: ["id"],
              },
              destination: {
                node: "Author",
                edgeColumns: ["publisher_id"],
                nodeColumns: ["id"],
              },
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.books` AS Book KEY (id) PROPERTIES (id, MEASURE(COUNT(id)) AS book_count),\n" +
            "  `defaultProject.defaultDataset.authors` AS Author KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")\n" +
            "EDGE TABLES (\n" +
            "  `defaultProject.defaultDataset.wrote` AS Wrote KEY (author_id, book_id) " +
            "SOURCE KEY (author_id) REFERENCES Author (id) " +
            "DESTINATION KEY (book_id) REFERENCES Book (id) NO PROPERTIES,\n" +
            "  `defaultProject.defaultDataset.books` AS WrittenByPrimaryAuthor KEY (id) " +
            "SOURCE KEY (id) REFERENCES Book (id) " +
            "DESTINATION KEY (publisher_id) REFERENCES Author (id) NO PROPERTIES\n" +
            ")",
        },
      ],
    },
    {
      testName: "graph.yaml job_labels and defaultJobLabels merge and preserve snake_case keys",
      workflowSettings: `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultJobLabels:
  env: dev
  team_name: graph_team
`,
      definitionFiles: [
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
job_labels:
  env: prod
  cost_center: finance
semantic_model:
- name: LabeledGraph
  entities:
  - name: Customer
    source: bigquery:defaultProject.defaultDataset.customers
    primary_key:
    - id
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "defaultDataset",
            name: "LabeledGraph",
            database: "defaultProject",
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "LabeledGraph",
            database: "defaultProject",
          },
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          actionDescriptor: {
            jobLabels: {
              env: "prod",
              team_name: "graph_team",
              cost_center: "finance",
            },
          },
          nodes: [
            {
              name: "Customer",
              dataSource: {
                schema: "defaultDataset",
                name: "customers",
                database: "defaultProject",
              },
              keys: ["id"],
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.customers` AS Customer KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")",
        },
      ],
    },
    {
      testName: "ref with schema and database overrides resolves the matching declaration",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "actions.yaml",
          contents: `
actions:
- declaration:
    name: books
    dataset: alt
    project: otherProject
- declaration:
    name: books
`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: RefWithOverridesGraph
  entities:
  - name: Book
    source:
      ref:
        name: books
        schema: alt
        database: otherProject
    primary_key:
    - id
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "defaultDataset",
            name: "RefWithOverridesGraph",
            database: "defaultProject",
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RefWithOverridesGraph",
            database: "defaultProject",
          },
          dependencyTargets: [
            {
              database: "otherProject",
              schema: "alt",
              name: "books",
            },
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          nodes: [
            {
              name: "Book",
              dataSource: {
                schema: "alt",
                name: "books",
                database: "otherProject",
              },
              keys: ["id"],
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `otherProject.alt.books` AS Book KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")",
        },
      ],
    },
    {
      testName: "graph-level dependOnDependencyAssertions pulls every ref's assertions",
      workflowSettings: VALID_WORKFLOW_SETTINGS_YAML,
      definitionFiles: [
        {
          name: "books.sqlx",
          contents: `config {
  type: "table",
  assertions: { rowConditions: ["id > 0"] }
}
select 1 as id`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
dependOnDependencyAssertions: true
semantic_model:
- name: GraphAssertDefaultGraph
  entities:
  - name: Book
    source:
      ref: books
    primary_key:
    - id
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "defaultDataset",
            name: "GraphAssertDefaultGraph",
            database: "defaultProject",
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "GraphAssertDefaultGraph",
            database: "defaultProject",
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "books",
            },
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "defaultDataset_books_assertions_rowConditions",
            },
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          nodes: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject",
              },
              keys: ["id"],
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.books` AS Book KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")",
        },
      ],
    },
    {
      testName: "ref to a table respects datasetSuffix, projectSuffix, and namePrefix",
      workflowSettings: `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
datasetSuffix: dev
projectSuffix: stg
namePrefix: pfx
`,
      definitionFiles: [
        {
          name: "books.sqlx",
          contents: `config {type: "table"}
select 1 as id`,
        },
        {
          name: "graph.yaml",
          contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: SuffixRefGraph
  entities:
  - name: Book
    source:
      ref: books
    primary_key:
    - id
`,
        },
      ],
      expectedPropertyGraphs: [
        {
          target: {
            schema: "defaultDataset_dev",
            name: "pfx_SuffixRefGraph",
            database: "defaultProject_stg",
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "SuffixRefGraph",
            database: "defaultProject",
          },
          dependencyTargets: [
            {
              schema: "defaultDataset_dev",
              name: "pfx_books",
              database: "defaultProject_stg",
            },
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          nodes: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset_dev",
                name: "pfx_books",
                database: "defaultProject_stg",
              },
              keys: ["id"],
            },
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject_stg.defaultDataset_dev.pfx_books` AS Book KEY (id) PROPERTIES ALL COLUMNS\n" +
            ")",
        },
      ],
    },
  ];

  testCases.forEach((testParameters) => {
    test(testParameters.testName, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, testParameters.workflowSettings);
      testParameters.definitionFiles.forEach((file) => {
        writeDefinitionFile(projectDir, file.name, file.contents);
      });

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      if (!testParameters.expectedGraph && !testParameters.expectedPropertyGraphs) {
        throw new Error(
          `Test case "${testParameters.testName}" must specify either expectedGraph or expectedPropertyGraphs`,
        );
      }

      if (testParameters.expectedGraph) {
        expect(asPlainGraph(result.compile?.compiledGraph)).deep.equals(
          asPlainObject(testParameters.expectedGraph),
        );
      }
      if (testParameters.expectedPropertyGraphs) {
        expect(result.compile?.compiledGraph?.graphErrors?.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile?.compiledGraph?.propertyGraphs)).deep.equals(
          asPlainObject(testParameters.expectedPropertyGraphs),
        );
      }
    });
  });

  test("more than one graph.yaml is rejected", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    const graphBody = `
version: 0.2.0.dev0/google
semantic_model:
- name: MultiGraph
  entities:
  - name: Node
    source: bigquery:defaultProject.defaultDataset.t
    primary_key:
    - id
`;
    writeDefinitionFile(projectDir, "graph.yaml", graphBody);
    writeDefinitionFile(projectDir, "subdir/graph.yaml", graphBody);

    const request = dataform.CoreExecutionRequest.create({
      compile: {
        compileConfig: {
          projectDir: fs.realpathSync(projectDir),
          filePaths: [
            "workflow_settings.yaml",
            "definitions/graph.yaml",
            "definitions/subdir/graph.yaml",
          ],
        },
      },
    });

    const result = runMainInVm(request);

    expect(asPlainGraph(result.compile?.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "At most one graph.yaml is allowed per project (found 2: " +
                "definitions/graph.yaml, definitions/subdir/graph.yaml). This " +
                "restriction may be relaxed in a future version.",
            ),
          ],
        },
        dataformCoreVersion: version,
        jitData: {},
      }),
    );
  });
});
