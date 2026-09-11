import { expect } from "chai";
import * as fs from "fs-extra";

import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import {
  asPlainObject,
  suite,
  test,
  writeDefinitionFile,
  writeWorkflowSettingsFile
} from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("property graphs", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const graphProjectConfig = {
    warehouse: "bigquery",
    defaultSchema: "defaultDataset",
    defaultDatabase: "defaultProject",
    defaultLocation: "US"
  };
  const graphStackTail = "\n    at CallSite {}".repeat(10);
  const graphError = (fileName: string, message: string, extra: object = {}) => ({
    fileName,
    message,
    stack: `Error: ${message}${graphStackTail}`,
    ...extra
  });

  test("valid graph.yaml compiles end-to-end", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: SimpleGraph
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
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
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "SimpleGraph",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            entities: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject"
                },
                keys: ["id"]
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("graph.yaml tags propagate into the compiled proto", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: TaggedGraph
tags:
- nightly
- reporting
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
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
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "TaggedGraph",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            tags: ["nightly", "reporting"],
            entities: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject"
                },
                keys: ["id"]
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("more than one graph.yaml is rejected", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    const graphBody = `
name: MultiGraph
entities:
- name: Node
  dataSourceString: defaultProject.defaultDataset.t
  keys:
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
            "definitions/subdir/graph.yaml"
          ]
        }
      }
    });

    const result = runMainInVm(request);

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "At most one graph.yaml is allowed per project (found 2: " +
                "definitions/graph.yaml, definitions/subdir/graph.yaml). This " +
                "restriction may be relaxed in a future version."
            )
          ]
        },
        dataformCoreVersion: version,
        jitData: {}
      })
    );
  });

  test("nodes-only graph compiles without EDGE TABLES", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: NodesOnly
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
- name: Product
  dataSourceString: defaultProject.defaultDataset.products
  keys:
  - sku
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "defaultDataset", name: "NodesOnly", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "NodesOnly",
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "NodesOnly",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            entities: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject"
                },
                keys: ["id"]
              },
              {
                name: "Product",
                dataSource: {
                  schema: "defaultDataset",
                  name: "products",
                  database: "defaultProject"
                },
                keys: ["sku"]
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id),\n" +
              "  `defaultProject.defaultDataset.products` AS Product KEY (sku)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("targetDataset overrides the schema on the graph target", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: CustomDsGraph
targetDataset:
  datasetId: customDs
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "customDs", name: "CustomDsGraph", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "customDs",
              name: "CustomDsGraph",
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "customDs",
              name: "CustomDsGraph",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            entities: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject"
                },
                keys: ["id"]
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("empty graph.yaml produces a compilation error", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "graph.yaml", "");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. Expected a top-level " +
                "object with 'name' and 'entities'."
            )
          ]
        },
        dataformCoreVersion: version,
        jitData: {}
      })
    );
  });

  test("graph.yaml with only a comment produces a compilation error", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "graph.yaml", "# nothing here\n");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. Expected a top-level " +
                "object with 'name' and 'entities'."
            )
          ]
        },
        dataformCoreVersion: version,
        jitData: {}
      })
    );
  });

  test("graph.yaml with a top-level scalar produces a compilation error", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "graph.yaml", "just a string\n");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph config is empty or malformed. Expected a top-level " +
                "object with 'name' and 'entities'."
            )
          ]
        },
        dataformCoreVersion: version,
        jitData: {}
      })
    );
  });

  test("graph.yaml missing entities produces a compilation error", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: EmptyGraph
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Property graph 'EmptyGraph' must declare at least one entity."
            )
          ]
        },
        dataformCoreVersion: version,
        jitData: {}
      })
    );
  });

  test("graph with relationships emits EDGE TABLES", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: RelGraph
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
- name: Order
  dataSourceString: defaultProject.defaultDataset.orders
  keys:
  - id
relationships:
- name: PlacedBy
  dataSourceString: defaultProject.defaultDataset.orders
  source:
    entity: Order
    joinKeys:
    - order_id
  destination:
    entity: Customer
    joinKeys:
    - customer_id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "defaultDataset", name: "RelGraph", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "RelGraph",
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "RelGraph",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            entities: [
              {
                name: "Customer",
                dataSource: {
                  schema: "defaultDataset",
                  name: "customers",
                  database: "defaultProject"
                },
                keys: ["id"]
              },
              {
                name: "Order",
                dataSource: {
                  schema: "defaultDataset",
                  name: "orders",
                  database: "defaultProject"
                },
                keys: ["id"]
              }
            ],
            relationships: [
              {
                name: "PlacedBy",
                dataSource: {
                  schema: "defaultDataset",
                  name: "orders",
                  database: "defaultProject"
                },
                source: {
                  entity: "Order",
                  relationshipColumns: ["order_id"],
                  entityColumns: ["id"]
                },
                destination: {
                  entity: "Customer",
                  relationshipColumns: ["customer_id"],
                  entityColumns: ["id"]
                }
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.customers` AS Customer KEY (id),\n" +
              "  `defaultProject.defaultDataset.orders` AS Order KEY (id)\n" +
              ")\n" +
              "EDGE TABLES (\n" +
              "  `defaultProject.defaultDataset.orders` AS PlacedBy " +
              "SOURCE KEY (order_id) REFERENCES Order (id) " +
              "DESTINATION KEY (customer_id) REFERENCES Customer (id)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("ref to declaration resolves entity dataSource and renders graphBody", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- declaration:
    name: books
`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: RefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "RefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "books"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" + "  `defaultProject.defaultDataset.books` AS Book KEY (id)\n" + ")"
        }
      ])
    );
  });

  test("ref with schema override resolves the matching declaration", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- declaration:
    name: books
    dataset: alt
- declaration:
    name: books
`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: RefWithSchemaGraph
entities:
- name: Book
  ref:
    name: books
    schema: alt
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "RefWithSchemaGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RefWithSchemaGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "alt",
              name: "books"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "alt",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody: "NODE TABLES (\n" + "  `defaultProject.alt.books` AS Book KEY (id)\n" + ")"
        }
      ])
    );
  });

  test("ref with includeDependentAssertions pulls the dependency's assertions", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {
  type: "table",
  assertions: { rowConditions: ["id > 0"] }
}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: AssertRefGraph
entities:
- name: Book
  ref:
    name: books
    includeDependentAssertions: true
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "AssertRefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "AssertRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "books"
            },
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "defaultDataset_books_assertions_rowConditions"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" + "  `defaultProject.defaultDataset.books` AS Book KEY (id)\n" + ")"
        }
      ])
    );
  });

  test("graph-level dependOnDependencyAssertions pulls every ref's assertions", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {
  type: "table",
  assertions: { rowConditions: ["id > 0"] }
}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: GraphAssertDefaultGraph
dependOnDependencyAssertions: true
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "GraphAssertDefaultGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "GraphAssertDefaultGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "books"
            },
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "defaultDataset_books_assertions_rowConditions"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" + "  `defaultProject.defaultDataset.books` AS Book KEY (id)\n" + ")"
        }
      ])
    );
  });

  test("missing ref emits a compilation error and leaves graphBody empty", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: MissingRefGraph
entities:
- name: Book
  ref: nonexistent
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    const missingRefTarget = {
      schema: "defaultDataset",
      name: "MissingRefGraph",
      database: "defaultProject"
    };
    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              "Missing dependency detected: Action " +
                '"defaultProject.defaultDataset.MissingRefGraph" depends on ' +
                '"{"name":"nonexistent","includeDependentAssertions":false}" ' +
                "which does not exist",
              {
                actionName: "defaultProject.defaultDataset.MissingRefGraph",
                actionTarget: missingRefTarget
              }
            )
          ]
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
            entities: [
              {
                name: "Book",
                keys: ["id"]
              }
            ]
          }
        ]
      })
    );
  });

  test("ref to a table respects datasetSuffix on the resolved dependency", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
datasetSuffix: dev
`
    );
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {type: "table"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: SuffixRefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset_dev",
            name: "SuffixRefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "SuffixRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              schema: "defaultDataset_dev",
              name: "books",
              database: "defaultProject"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset_dev",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset_dev.books` AS Book KEY (id)\n" +
            ")"
        }
      ])
    );
  });

  test("ref with database override resolves the matching declaration", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- declaration:
    name: books
    project: otherProject
- declaration:
    name: books
`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: RefWithDatabaseGraph
entities:
- name: Book
  ref:
    name: books
    database: otherProject
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "RefWithDatabaseGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RefWithDatabaseGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "otherProject",
              schema: "defaultDataset",
              name: "books"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                database: "otherProject",
                schema: "defaultDataset",
                name: "books"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" + "  `otherProject.defaultDataset.books` AS Book KEY (id)\n" + ")"
        }
      ])
    );
  });

  test("relationship ref resolves through the full pipeline", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- declaration:
    name: wrote
`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: RelationshipRefGraph
entities:
- name: Book
  dataSourceString: defaultProject.defaultDataset.books
  keys:
  - id
- name: Author
  dataSourceString: defaultProject.defaultDataset.authors
  keys:
  - id
relationships:
- name: WrittenBy
  ref: wrote
  keys:
  - author_id
  - book_id
  source:
    entity: Book
    joinKeys:
    - book_id
  destination:
    entity: Author
    joinKeys:
    - author_id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "RelationshipRefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "RelationshipRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "wrote"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "books"
              },
              keys: ["id"]
            },
            {
              name: "Author",
              dataSource: {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "authors"
              },
              keys: ["id"]
            }
          ],
          relationships: [
            {
              name: "WrittenBy",
              dataSource: {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "wrote"
              },
              keys: ["author_id", "book_id"],
              source: {
                entity: "Book",
                relationshipColumns: ["book_id"],
                entityColumns: ["id"]
              },
              destination: {
                entity: "Author",
                relationshipColumns: ["author_id"],
                entityColumns: ["id"]
              }
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.books` AS Book KEY (id),\n" +
            "  `defaultProject.defaultDataset.authors` AS Author KEY (id)\n" +
            ")\n" +
            "EDGE TABLES (\n" +
            "  `defaultProject.defaultDataset.wrote` AS WrittenBy " +
            "KEY (author_id, book_id) " +
            "SOURCE KEY (book_id) REFERENCES Book (id) " +
            "DESTINATION KEY (author_id) REFERENCES Author (id)\n" +
            ")"
        }
      ])
    );
  });

  test("ref to a view resolves and picks up datasetSuffix", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
datasetSuffix: dev
`
    );
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {type: "view"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: ViewRefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset_dev",
            name: "ViewRefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "ViewRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset_dev",
              name: "books"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                database: "defaultProject",
                schema: "defaultDataset_dev",
                name: "books"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset_dev.books` AS Book KEY (id)\n" +
            ")"
        }
      ])
    );
  });

  test("ambiguous ref emits a compilation error", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- declaration:
    name: books
    dataset: one
- declaration:
    name: books
    dataset: two
`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: AmbiguousRefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    const declOneTarget = { schema: "one", name: "books", database: "defaultProject" };
    const declTwoTarget = { schema: "two", name: "books", database: "defaultProject" };
    const graphTarget = {
      schema: "defaultDataset",
      name: "AmbiguousRefGraph",
      database: "defaultProject"
    };
    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError(
              "definitions/graph.yaml",
              `Ambiguous Action name: {"name":"books","includeDependentAssertions":false}. ` +
                "Did you mean one of: one.books, two.books.",
              {
                actionName: "defaultProject.defaultDataset.AmbiguousRefGraph",
                actionTarget: graphTarget
              }
            )
          ]
        },
        dataformCoreVersion: version,
        targets: [declOneTarget, declTwoTarget, graphTarget],
        jitData: {},
        declarations: [
          { target: declOneTarget, canonicalTarget: declOneTarget },
          { target: declTwoTarget, canonicalTarget: declTwoTarget }
        ],
        propertyGraphs: [
          {
            target: graphTarget,
            canonicalTarget: graphTarget,
            fileName: "definitions/graph.yaml",
            description: "",
            disabled: false,
            entities: [{ name: "Book", keys: ["id"] }]
          }
        ]
      })
    );
  });

  test("ref to a table respects projectSuffix on the resolved dependency", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
projectSuffix: dev
`
    );
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {type: "table"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: ProjectSuffixRefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "ProjectSuffixRefGraph",
            database: "defaultProject_dev"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "ProjectSuffixRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              schema: "defaultDataset",
              name: "books",
              database: "defaultProject_dev"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject_dev"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject_dev.defaultDataset.books` AS Book KEY (id)\n" +
            ")"
        }
      ])
    );
  });

  test("ref to a table respects namePrefix on the resolved dependency", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
namePrefix: pfx
`
    );
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {type: "table"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: NamePrefixRefGraph
entities:
- name: Book
  ref: books
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "pfx_NamePrefixRefGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "NamePrefixRefGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            {
              schema: "defaultDataset",
              name: "pfx_books",
              database: "defaultProject"
            }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "pfx_books",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.pfx_books` AS Book KEY (id)\n" +
            ")"
        }
      ])
    );
  });

  test("graph target colliding with a table target is flagged as duplicate", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "collision.sqlx",
      `config {type: "table", name: "CollisionName"}
select 1 as a`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: CollisionName
entities:
- name: Customer
  dataSourceString: defaultProject.defaultDataset.customers
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    const collisionTarget = {
      schema: "defaultDataset",
      name: "CollisionName",
      database: "defaultProject"
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
    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {
          compilationErrors: [
            graphError("definitions/collision.sqlx", duplicateActionMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget
            }),
            graphError("definitions/collision.sqlx", duplicateCanonicalMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget
            }),
            graphError("definitions/graph.yaml", duplicateActionMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget
            }),
            graphError("definitions/graph.yaml", duplicateCanonicalMessage, {
              actionName: collisionActionName,
              actionTarget: collisionTarget
            })
          ]
        },
        dataformCoreVersion: version,
        targets: [collisionTarget, collisionTarget],
        jitData: {}
      })
    );
  });
  test("graph.yaml accepts snake_case keys per BQ spec", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: SnakeGraph
description: end to end snake case
target_dataset:
  project_id: defaultProject
  dataset_id: defaultDataset
entities:
- name: Account
  data_source_string: defaultProject.defaultDataset.accounts
  keys:
  - id
  fields:
    import_all: true
    except:
    - secret
relationships:
- name: Owns
  data_source_string: defaultProject.defaultDataset.ownership
  source:
    entity: Account
    join_keys:
      relationship_columns:
      - owner_id
  destination:
    entity: Account
    join_keys:
      relationship_columns:
      - owned_id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        projectConfig: graphProjectConfig,
        graphErrors: {},
        dataformCoreVersion: version,
        targets: [{ schema: "defaultDataset", name: "SnakeGraph", database: "defaultProject" }],
        jitData: {},
        propertyGraphs: [
          {
            target: {
              schema: "defaultDataset",
              name: "SnakeGraph",
              database: "defaultProject"
            },
            canonicalTarget: {
              schema: "defaultDataset",
              name: "SnakeGraph",
              database: "defaultProject"
            },
            fileName: "definitions/graph.yaml",
            description: "end to end snake case",
            disabled: false,
            entities: [
              {
                name: "Account",
                dataSource: {
                  schema: "defaultDataset",
                  name: "accounts",
                  database: "defaultProject"
                },
                keys: ["id"],
                labels: [
                  {
                    name: "Account",
                    description: "",
                    importAll: true,
                    importExcept: ["secret"],
                    isDefault: true
                  }
                ]
              }
            ],
            relationships: [
              {
                name: "Owns",
                dataSource: {
                  schema: "defaultDataset",
                  name: "ownership",
                  database: "defaultProject"
                },
                source: {
                  entity: "Account",
                  relationshipColumns: ["owner_id"],
                  entityColumns: ["id"]
                },
                destination: {
                  entity: "Account",
                  relationshipColumns: ["owned_id"],
                  entityColumns: ["id"]
                }
              }
            ],
            graphBody:
              "NODE TABLES (\n" +
              "  `defaultProject.defaultDataset.accounts` AS Account KEY (id) " +
              "DEFAULT LABEL PROPERTIES ARE ALL COLUMNS EXCEPT (secret)\n" +
              ")\n" +
              "EDGE TABLES (\n" +
              "  `defaultProject.defaultDataset.ownership` AS Owns " +
              "SOURCE KEY (owner_id) REFERENCES Account (id) " +
              "DESTINATION KEY (owned_id) REFERENCES Account (id)\n" +
              ")"
          }
        ]
      })
    );
  });

  test("mixed ref and dataSourceString: only ref target appears in dependencyTargets", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "books.sqlx",
      `config {type: "table"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "authors.sqlx",
      `config {type: "table"}
select 1 as id`
    );
    writeDefinitionFile(
      projectDir,
      "graph.yaml",
      `
name: MixedRefStringGraph
entities:
- name: Book
  ref: books
  keys:
  - id
- name: Author
  dataSourceString: defaultProject.defaultDataset.authors
  keys:
  - id
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.propertyGraphs)).deep.equals(
      asPlainObject([
        {
          target: {
            schema: "defaultDataset",
            name: "MixedRefStringGraph",
            database: "defaultProject"
          },
          canonicalTarget: {
            schema: "defaultDataset",
            name: "MixedRefStringGraph",
            database: "defaultProject"
          },
          dependencyTargets: [
            { database: "defaultProject", schema: "defaultDataset", name: "books" }
          ],
          fileName: "definitions/graph.yaml",
          description: "",
          disabled: false,
          entities: [
            {
              name: "Book",
              dataSource: {
                schema: "defaultDataset",
                name: "books",
                database: "defaultProject"
              },
              keys: ["id"]
            },
            {
              name: "Author",
              dataSource: {
                schema: "defaultDataset",
                name: "authors",
                database: "defaultProject"
              },
              keys: ["id"]
            }
          ],
          graphBody:
            "NODE TABLES (\n" +
            "  `defaultProject.defaultDataset.books` AS Book KEY (id),\n" +
            "  `defaultProject.defaultDataset.authors` AS Author KEY (id)\n" +
            ")"
        }
      ])
    );
  });
});
