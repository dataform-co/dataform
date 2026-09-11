import { expect } from "chai";

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

suite("sqlx and JS API config options", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  // If change, then change test "action configs assertions can be loaded".
  const assertionConfig = `{
  type: "assertion",
  name: "name",
  schema: "dataset",
  database: "project",
  dependencies: ["operation"],
  tags: ["tagA", "tagB"],
  disabled: true,
  description: "description",
  hermetic: true,
  dependOnDependencyAssertions: true,
  metadata: {
      overview: "assertion overview",
      extraProperties: {
          fields: {
              priority: { stringValue: "high" }
          }
      }
  }
}`;
  [
    {
      filename: "assertion.sqlx",
      fileContents: `
config ${assertionConfig}
SELECT 1`
    },
    {
      filename: "assertion.js",
      fileContents: `assert("name", ${assertionConfig}).query(ctx => \`\n\nSELECT 1\`)`
    }
  ].forEach(testParameters => {
    test(`for assertions configured in a ${testParameters.filename} file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, testParameters.filename, testParameters.fileContents);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
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
            actionDescriptor: {
              description: "description",
              metadata: {
                overview: "assertion overview",
                extraProperties: {
                  fields: {
                    priority: { stringValue: "high" }
                  }
                }
              }
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation"
              }
            ],
            disabled: true,
            fileName: `definitions/${testParameters.filename}`,
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "\n\nSELECT 1"
          }
        ])
      );
    });
  });

  test("assertions can be configured with a plain object for extraProperties", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "assertion.sqlx",
      `config {
    type: "assertion",
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
      asPlainObject(result.compile.compiledGraph.assertions[0].actionDescriptor.metadata)
    ).deep.equals({
      extraProperties: {
        fields: {
          priority: { stringValue: "high" }
        }
      }
    });
  });

  ["table", "view", "incremental"].forEach(tableType => {
    [`"fieldValue"`, `["fieldValue"]`].forEach(uniqueKeyField => {
      test(`for ${tableType} built-in assertions uniqueKey with value ${uniqueKeyField}`, () => {
        // The \`uniqueKey\` built in assertion field cannot be present at the same time as
        // \`uniqueKeys\`, so it is tested separately here.
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "filename.sqlx",
          `
config {
    type: "${tableType}",
    assertions: {
    uniqueKey: ${uniqueKeyField},
    },
}
SELECT 2`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "defaultDataset_filename_assertions_uniqueKey_0"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "defaultDataset_filename_assertions_uniqueKey_0"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "filename"
              }
            ],
            fileName: "definitions/filename.sqlx",
            parentAction: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "filename"
            },
            query:
              "\nSELECT\n  *\nFROM (\n  SELECT\n    fieldValue,\n    COUNT(1) AS index_row_count\n  FROM `defaultProject.defaultDataset.filename`\n  GROUP BY fieldValue\n  ) AS data\nWHERE index_row_count > 1\n"
          }
        ]);
      });
    });
  });
});
