import { expect } from "chai";

import { dataform, google } from "df/protos/ts";
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
  VALID_WORKFLOW_SETTINGS_YAML,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

suite("javascript API", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  suite("publish", () => {
    ["table", "view", "incremental"].forEach(tableType => {
      [
        WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
        {
          ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
          defaultProject: "defaultProject"
        },
        { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
      ].forEach(projectConfig => {
        test(
          `publish for table type ${tableType}, with project suffix ` +
            `'${projectConfig.projectSuffix}', dataset suffix ` +
            `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
          () => {
            const projectDir = tmpDirFixture.createNewTmpDir();
            writeWorkflowSettingsFile(projectDir, projectConfig);
            writeDefinitionFile(
              projectDir,
              "publish.js",
              `
publish("name", {
  type: "${tableType}",
}).query(_ => "SELECT 1")
  .preOps(_ => ["pre_op"])
  .postOps(_ => ["post_op"])
  .database("otherProject")
  .schema("otherDataset")`
            );

            const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
              asPlainObject([
                {
                  type: tableType,
                  hermeticity: "NON_HERMETIC",
                  target: {
                    database: projectConfig.projectSuffix
                      ? `otherProject_${projectConfig.projectSuffix}`
                      : "otherProject",
                    schema: projectConfig.datasetSuffix
                      ? `otherDataset_${projectConfig.datasetSuffix}`
                      : "otherDataset",
                    name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                  },
                  canonicalTarget: {
                    database: projectConfig.defaultProject,
                    schema: projectConfig.defaultDataset,
                    name: "name"
                  },
                  disabled: false,
                  enumType: tableType.toUpperCase(),
                  fileName: "definitions/publish.js",
                  query: "SELECT 1",
                  postOps: ["post_op"],
                  preOps: ["pre_op"],
                  ...(tableType === "incremental"
                    ? {
                        incrementalPostOps: ["post_op"],
                        incrementalPreOps: ["pre_op"],
                        incrementalQuery: "SELECT 1",
                        incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
                        protected: false,
                        onSchemaChange: "IGNORE"
                      }
                    : {})
                }
              ])
            );
          }
        );
      });

      test("ref resolved correctly", () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "operation.sqlx",
          `
config {
  hasOutput: true
}
SELECT 1`
        );
        writeDefinitionFile(
          projectDir,
          "publish.js",
          `
publish("name", {
  type: "${tableType}",
}).query(ctx => \`SELECT * FROM \${ctx.ref('operation')}\`)`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
          {
            canonicalTarget: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                name: "operation",
                schema: "defaultDataset"
              }
            ],
            disabled: false,
            enumType: tableType.toUpperCase(),
            fileName: "definitions/publish.js",
            hermeticity: "NON_HERMETIC",
            query: "SELECT * FROM `defaultProject.defaultDataset.operation`",
            target: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            },
            type: tableType,
            ...(tableType === "incremental"
              ? {
                  incrementalQuery: "SELECT * FROM `defaultProject.defaultDataset.operation`",
                  incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
                  protected: false,
                  onSchemaChange: "IGNORE"
                }
              : {})
          }
        ]);
      });

      if (tableType === "incremental") {
        test("protected resolved correctly", () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
          writeDefinitionFile(
            projectDir,
            "publish.js",
            `
publish("name", {
  type: "incremental",
}).query(_ => "SELECT 1")
  .protected()`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "name",
                schema: "defaultDataset"
              },
              disabled: false,
              enumType: tableType.toUpperCase(),
              fileName: "definitions/publish.js",
              hermeticity: "NON_HERMETIC",
              query: "SELECT 1",
              target: {
                database: "defaultProject",
                name: "name",
                schema: "defaultDataset"
              },
              type: tableType,
              incrementalQuery: "SELECT 1",
              protected: true,
              onSchemaChange: "IGNORE",
              incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED"
            }
          ]);
        });
      }
    });
  });

  suite("operate", () => {
    [
      WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
      {
        ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
        defaultProject: "defaultProject"
      },
      { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
    ].forEach(projectConfig => {
      test(
        `operate with project suffix ` +
          `'${projectConfig.projectSuffix}', dataset suffix ` +
          `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
        () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(projectDir, projectConfig);
          writeDefinitionFile(
            projectDir,
            "operate.js",
            `
operate("name", {
  type: "operations",
}).queries(_ => ["SELECT 1", "SELECT 2"])
  .database("otherProject")
  .schema("otherDataset")`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
            asPlainObject([
              {
                target: {
                  database: projectConfig.projectSuffix
                    ? `otherProject_${projectConfig.projectSuffix}`
                    : "otherProject",
                  schema: projectConfig.datasetSuffix
                    ? `otherDataset_${projectConfig.datasetSuffix}`
                    : "otherDataset",
                  name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                },
                canonicalTarget: {
                  database: projectConfig.defaultProject,
                  schema: projectConfig.defaultDataset,
                  name: "name"
                },
                fileName: "definitions/operate.js",
                hermeticity: "NON_HERMETIC",
                queries: ["SELECT 1", "SELECT 2"]
              }
            ])
          );
        }
      );
    });

    test("ref resolved correctly", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "table.sqlx", `config {type: "table"} SELECT 1`);
      writeDefinitionFile(
        projectDir,
        "operate.js",
        `
operate("name", {
  type: "operations",
}).queries(ctx => [\`SELECT * FROM \${ctx.ref('table')}\`])`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals([
        {
          canonicalTarget: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              name: "table",
              schema: "defaultDataset"
            }
          ],
          fileName: "definitions/operate.js",
          hermeticity: "NON_HERMETIC",
          queries: ["SELECT * FROM `defaultProject.defaultDataset.table`"],
          target: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          }
        }
      ]);
    });
  });

  suite("assert", () => {
    [
      WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
      {
        ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
        defaultProject: "defaultProject"
      },
      { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
    ].forEach(projectConfig => {
      test(
        `assert with project suffix ` +
          `'${projectConfig.projectSuffix}', dataset suffix ` +
          `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
        () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(projectDir, projectConfig);
          writeDefinitionFile(
            projectDir,
            "assert.js",
            `
assert("name", {
  type: "operations",
}).query(_ => "SELECT 1")
  .database("otherProject")
  .schema("otherDataset")`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
            asPlainObject([
              {
                target: {
                  database: projectConfig.projectSuffix
                    ? `otherProject_${projectConfig.projectSuffix}`
                    : "otherProject",
                  schema: projectConfig.datasetSuffix
                    ? `otherDataset_${projectConfig.datasetSuffix}`
                    : "otherDataset",
                  name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                },
                canonicalTarget: {
                  database: projectConfig.defaultProject,
                  schema: projectConfig.defaultDataset,
                  name: "name"
                },
                fileName: "definitions/assert.js",
                query: "SELECT 1"
              }
            ])
          );
        }
      );
    });

    test("ref resolved correctly", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "table.sqlx", `config {type: "table"} SELECT 1`);
      writeDefinitionFile(
        projectDir,
        "assert.js",
        `
assert("name", {
  type: "assert",
}).query(ctx => \`SELECT * FROM \${ctx.ref('table')}\`)`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
        {
          canonicalTarget: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              name: "table",
              schema: "defaultDataset"
            }
          ],
          fileName: "definitions/assert.js",
          query: "SELECT * FROM `defaultProject.defaultDataset.table`",
          target: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          }
        }
      ]);
    });

    test("jitCode correctly populates the jitCode field", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "assert.js",
        `
assert("name").jitCode(ctx => "jit");`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
        {
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          },
          fileName: "definitions/assert.js",
          jitCode: 'ctx => "jit"',
          target: {
            database: "defaultProject",
            name: "name",
            schema: "defaultDataset"
          }
        }
      ]);
    });

    test("fails when both jitCode and query are set on an assertion", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "assert.js",
        `
assert("name").query("SELECT 1").jitCode(ctx => "jit");`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals(["Assertion may set either .jitCode() or .query(), but not both."]);
    });

    test("assert API returns disabled assertions when disableAssertions is true", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "table.sqlx", `config {type: "table"} SELECT 1`);
      writeDefinitionFile(
        projectDir,
        "assert.js",
        `
assert("name", {
  type: "assert",
}).query(ctx => \`SELECT * FROM \${ctx.ref('table')}\`)`
      );

      const coreRequest = coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({
          disableAssertions: true
        })
      );
      const result = runMainInVm(coreRequest);

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                name: "table",
                schema: "defaultDataset"
              }
            ],
            disabled: true,
            fileName: "definitions/assert.js",
            query: "SELECT * FROM `defaultProject.defaultDataset.table`",
            target: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            }
          }
        ])
      );
      expect(result.compile.compiledGraph.tables.length).equals(1);
    });
  });

  suite("jitData", () => {
    test("jitData is added to the compiled graph", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "jit.js",
        `
dataform.jitData("key", {
  "number": 123,
  "string": "value",
  "boolean": true,
  "struct": {
    "nestedKey": "nestedValue"
  },
  "list": [
    "a",
    "b",
    "c"
  ],
  "null": null,
  "undef": undefined,
});`
      );
      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.jitData).to.deep.equal(
        google.protobuf.Struct.create({
          fields: {
            key: google.protobuf.Value.create({
              structValue: google.protobuf.Struct.create({
                fields: {
                  number: google.protobuf.Value.create({ numberValue: 123 }),
                  string: google.protobuf.Value.create({ stringValue: "value" }),
                  boolean: google.protobuf.Value.create({ boolValue: true }),
                  struct: google.protobuf.Value.create({
                    structValue: google.protobuf.Struct.create({
                      fields: {
                        nestedKey: google.protobuf.Value.create({ stringValue: "nestedValue" })
                      }
                    })
                  }),
                  list: google.protobuf.Value.create({
                    listValue: google.protobuf.ListValue.create({
                      values: [
                        google.protobuf.Value.create({ stringValue: "a" }),
                        google.protobuf.Value.create({ stringValue: "b" }),
                        google.protobuf.Value.create({ stringValue: "c" })
                      ]
                    })
                  }),
                  null: google.protobuf.Value.create({
                    nullValue: google.protobuf.NullValue.NULL_VALUE
                  }),
                  undef: google.protobuf.Value.create({
                    nullValue: google.protobuf.NullValue.NULL_VALUE
                  })
                }
              })
            })
          }
        })
      );
    });

    test("jitData with duplicate key throws error", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "jit.js",
        `
dataform.jitData("key", 1);
dataform.jitData("key", 2);
`
      );
      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.map(e => e.message)
      ).to.deep.equal(["JiT context data with key key already exists."]);
    });

    test("jitData with unsupported type throws error", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "jit.js",
        `
dataform.jitData("key", {test: () => {}});
`
      );
      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.map(e => e.message)
      ).to.deep.equal(["Unsupported value: () => {}"]);
    });
  });
  suite("markdown in description", () => {
    test("markdown contents added to description in sqlx", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "descriptions.md", `# This table contains data about`);
      writeDefinitionFile(
        projectDir,
        "table.sqlx",
        `config {
            type: "table",
            description: getContents('./descriptions.md'),
          }
            SELECT 1 AS test`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.tables[0].actionDescriptor.description).to.equal(
        `# This table contains data about`
      );
    });

    test("throws error for invalid missing markedown", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "table.sqlx",
        `config {
            type: "table",
            description: getContents('./nonexistent.md'),
          }
            SELECT 1 AS test`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).to.include(
        "nonexistent.md"
      );
    });

    test("throws error for file outisde of rootDir", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "table.sqlx",
        `config {
            type: "table",
            description: getContents('../../description.md'),
          }
            SELECT 1 AS test`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).to.include(
        "outside the project directory"
      );
    });
  });
  suite("invalid options", () => {
    [
      {
        testName: "partitionBy invalid for BigQuery non-materialized views",
        fileContents: `
publish("name", {
  type: "view",
  materialized: false,
  bigquery: {
    partitionBy: "some_partition"
  }
})`,
        expectedError: "partitionBy/clusterBy can be applied only to materialized views"
      },
      {
        testName: "clusterBy invalid for BigQuery non-materialized views",
        fileContents: `
publish("name", {
  type: "view",
  materialized: false,
  bigquery: {
    clusterBy: ["some_cluster"]
  }
})`,
        expectedError: "partitionBy/clusterBy can be applied only to materialized views"
      },
      {
        testName: "partitionExpirationDays invalid for BigQuery views",
        fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
        expectedError:
          'Unexpected property "partitionExpirationDays" in BigQuery view config. Supported properties are: ["labels","additionalOptions","partitionBy","clusterBy"]'
      },
      {
        testName: "requirePartitionFilter invalid for BigQuery views",
        fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    requirePartitionFilter: true
  }
})`,
        expectedError:
          'Unexpected property "requirePartitionFilter" in BigQuery view config. Supported properties are: ["labels","additionalOptions","partitionBy","clusterBy"]'
      },
      {
        testName: "partitionExpirationDays invalid for BigQuery materialized views",
        fileContents: `
publish("name", {
  type: "view",
  materialized: true,
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
        expectedError:
          'Unexpected property "partitionExpirationDays" in BigQuery view config. Supported properties are: ["labels","additionalOptions","partitionBy","clusterBy"]'
      },
      {
        testName: "requirePartitionFilter invalid for BigQuery materialized views",
        fileContents: `
publish("name", {
  type: "view",
  materialized: true,
  bigquery: {
    requirePartitionFilter: true
  }
})`,
        expectedError:
          'Unexpected property "requirePartitionFilter" in BigQuery view config. Supported properties are: ["labels","additionalOptions","partitionBy","clusterBy"]'
      },
      {
        testName: "materialized invalid for BigQuery tables",
        fileContents: `
publish("name", {
  type: "table",
  materialized: true,
})`,
        expectedError:
          'Unexpected property "materialized", or property value type of "boolean" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfig-TableConfig for allowed properties.'
      },
      {
        testName: "partitionExpirationDays invalid for BigQuery tables",
        fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
        expectedError:
          "requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables"
      },
      {
        testName: "duplicate partitionExpirationDays is invalid",
        fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionBy: "partition",
    partitionExpirationDays: 1,
    additionalOptions: {
      partition_expiration_days: "7"
    }
  }
})`,
        expectedError: "partitionExpirationDays has been declared twice"
      },
      {
        testName: "duplicate requirePartitionFilter is invalid",
        fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionBy: "partition",
    requirePartitionFilter: true,
    additionalOptions: {
      require_partition_filter: "false"
    }
  }
})`,
        expectedError: "requirePartitionFilter has been declared twice"
      }
    ].forEach(testParameters => {
      test(testParameters.testName, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
        writeDefinitionFile(projectDir, "file.js", testParameters.fileContents);

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(
          result.compile.compiledGraph.graphErrors.compilationErrors.map(
            compilationError => compilationError.message
          )
        ).deep.equals([testParameters.expectedError]);
      });
    });
  });

  suite(`legacy publish().type() can still be called`, () => {
    ["table", "incremental", "view"].forEach(fromType => {
      ["table", "incremental", "view"].forEach(toType => {
        test(`from type ${fromType} to ${toType}`, () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(
            projectDir,
            WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset
          );
          writeDefinitionFile(
            projectDir,
            "publish.js",
            `
publish("name", {type: "${fromType}", schema: "schemaOverride"}).type("${toType}")`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          const tables = asPlainObject(result.compile.compiledGraph.tables);
          expect(tables.length).equals(1);
          expect(tables[0].type).equals(toType);
          expect(tables[0].enumType).equals(toType.toUpperCase());

          // Config options are carried over to the new table type, where possible.
          expect(tables[0].target.schema).equals("schemaOverride");
        });
      });
    });
  });
});
