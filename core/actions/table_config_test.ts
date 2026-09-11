import { expect } from "chai";

import { exampleActionDescriptor, exampleBuiltInAssertions } from "df/core/actions/index_test";
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

suite("table sqlx and JS API config options", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  const tableConfig = `{
    type: "table",
    name: "name",
    schema: "dataset",
    database: "project",
    dependencies: ["operation"],
    tags: ["tag1", "tag2"],
    disabled: true,
    description: "description",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
    bigquery: {
        partitionBy: "partitionBy",
        partitionExpirationDays: 1,
        requirePartitionFilter: true,
        clusterBy: ["clusterBy"],
        labels: {"key": "val"},
        additionalOptions: {
        option1Key: "option1",
        option2Key: "option2",
        }
    },
    ${exampleBuiltInAssertions.inputAssertionBlock}
    dependOnDependencyAssertions: true,
    hermetic: true,
    reservation: "reservation",
    metadata: {
        overview: "table overview",
        extraProperties: {
            fields: {
                priority: { stringValue: "high" }
            }
        }
    }
}`;

  [
    {
      filename: "table.sqlx",
      fileContents: `
config ${tableConfig}
SELECT 1`
    },
    {
      filename: "table.js",
      fileContents: `publish("name", ${tableConfig}).query(ctx => \`\n\nSELECT 1\`)`
    }
  ].forEach(testParameters => {
    test(`for tables configured in a ${testParameters.filename} file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, testParameters.filename, testParameters.fileContents);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "project",
            schema: "dataset",
            name: "name"
          },
          canonicalTarget: {
            database: "project",
            schema: "dataset",
            name: "name"
          },
          type: "table",
          disabled: true,
          hermeticity: "HERMETIC",
          bigquery: {
            additionalOptions: {
              option1Key: "option1",
              option2Key: "option2"
            },
            clusterBy: ["clusterBy"],
            labels: {
              key: "val"
            },
            partitionBy: "partitionBy",
            partitionExpirationDays: 1,
            requirePartitionFilter: true
          },
          tags: ["tag1", "tag2"],
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "operation"
            }
          ],
          enumType: "TABLE",
          fileName: `definitions/${testParameters.filename}`,
          query: "\n\nSELECT 1",
          actionDescriptor: {
            ...exampleActionDescriptor.outputActionDescriptor,
            reservation: "reservation",
            // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
            bigqueryLabels: {
              key: "val"
            },
            metadata: {
              overview: "table overview",
              extraProperties: {
                fields: {
                  priority: { stringValue: "high" }
                }
              }
            }
          }
        }
      ]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        exampleBuiltInAssertions.outputAssertions(testParameters.filename)
      );
    });
  });

  test("tables can be configured with a plain object for extraProperties", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "table.sqlx",
      `config {
    type: "table",
    metadata: {
        extraProperties: {
            priority: "high"
        }
    }
}
SELECT 1`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(
      asPlainObject(result.compile.compiledGraph.tables[0].actionDescriptor.metadata)
    ).deep.equals({
      extraProperties: {
        fields: {
          priority: { stringValue: "high" }
        }
      }
    });
  });

  test("tables can be configured with a Struct already for extraProperties", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "table.sqlx",
      `config {
    type: "table",
    metadata: {
        extraProperties: {
            fields: {
                priority: { stringValue: "high" },
                glossary_terms: {
                    listValue: {
                        values: [
                            {
                                structValue: {
                                    fields: {
                                        column_name: { stringValue: "trip_id" },
                                        project: { stringValue: "project_identifier" },
                                        location: { stringValue: "us-central1" }
                                    }
                                }
                            },
                            {
                                structValue: {
                                    fields: {
                                        project: { stringValue: "project_identifier" },
                                        glossary_id: { stringValue: "jebmjilij-9c85ee94" }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
}
SELECT 1`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(
      asPlainObject(result.compile.compiledGraph.tables[0].actionDescriptor.metadata)
    ).deep.equals({
      extraProperties: {
        fields: {
          priority: { stringValue: "high" },
          glossary_terms: {
            listValue: {
              values: [
                {
                  structValue: {
                    fields: {
                      column_name: { stringValue: "trip_id" },
                      project: { stringValue: "project_identifier" },
                      location: { stringValue: "us-central1" }
                    }
                  }
                },
                {
                  structValue: {
                    fields: {
                      project: { stringValue: "project_identifier" },
                      glossary_id: { stringValue: "jebmjilij-9c85ee94" }
                    }
                  }
                }
              ]
            }
          }
        }
      }
    });
  });

  test("tables can be configured with a complex nested plain object for extraProperties", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "table.sqlx",
      `config {
      type: "table",
      metadata: {
        overview: "The test overview",
        extraProperties: {
          glossary_terms: [
            {
              column_name: "trip_id",
              project: "project_identifier",
              location: "us-central1"
            },
            {
              project: "project_identifier",
              glossary_id: "jebmjilij-9c85ee94"
            }
          ],
          generic: {
            system: "my custom system value",
            type: "my custom type value"
          }
        }
      }
    }
    SELECT 1`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const metadata = result.compile.compiledGraph.tables[0].actionDescriptor.metadata;
    expect(metadata.overview).equals("The test overview");

    expect(asPlainObject(metadata.extraProperties)).deep.equals({
      fields: {
        glossary_terms: {
          listValue: {
            values: [
              {
                structValue: {
                  fields: {
                    column_name: { stringValue: "trip_id" },
                    project: { stringValue: "project_identifier" },
                    location: { stringValue: "us-central1" }
                  }
                }
              },
              {
                structValue: {
                  fields: {
                    project: { stringValue: "project_identifier" },
                    glossary_id: { stringValue: "jebmjilij-9c85ee94" }
                  }
                }
              }
            ]
          }
        },
        generic: {
          structValue: {
            fields: {
              system: { stringValue: "my custom system value" },
              type: { stringValue: "my custom type value" }
            }
          }
        }
      }
    });
  });
});
