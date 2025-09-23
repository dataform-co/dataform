// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("assertion", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`assertions can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        // If change, then change test "sqlx config options checks for assertions".
        `
actions:
- assertion:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
      - name: operation
        dataset: defaultDataset
        project: defaultProject
    filename: action.sql
    tags:
      - tagA
      - tagB
    disabled: true,
    description: description
    hermetic: true,
    dependOnDependencyAssertions: true`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");

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
              description: "description"
            },
            disabled: true,
            fileName: "definitions/action.sql",
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "SELECT 1",
            dependencyTargets: [
              {
                name: "operation",
                schema: "defaultDataset",
                database: "defaultProject"
              }
            ]
          }
        ])
      );
    });
  });

  suite("sqlx and JS API config options", () => {
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
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
        fs.writeFileSync(
          path.join(projectDir, `definitions/${testParameters.filename}`),
          testParameters.fileContents
        );

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
                description: "description"
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

    ["table", "view", "incremental"].forEach(tableType => {
      [`"fieldValue"`, `["fieldValue"]`].forEach(uniqueKeyField => {
        test(`for ${tableType} built-in assertions uniqueKey with value ${uniqueKeyField}`, () => {
          // The `uniqueKey` built in assertion field cannot be present at the same time as
          // `uniqueKeys`, so it is tested separately here.
          const projectDir = tmpDirFixture.createNewTmpDir();
          fs.writeFileSync(
            path.join(projectDir, "workflow_settings.yaml"),
            VALID_WORKFLOW_SETTINGS_YAML
          );
          fs.mkdirSync(path.join(projectDir, "definitions"));
          fs.writeFileSync(
            path.join(projectDir, "definitions/filename.sqlx"),
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

  test(`action config options`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "definitions/filename.sql"), "SELECT 1");
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- assertion:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
    - name: operation
    filename: filename.sql
    tags:
    - tagA
    - tagB
    disabled: true
    description: description
    hermetic: true
    dependOnDependencyAssertions: true
`
    );

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
            description: "description"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "operation"
            }
          ],
          disabled: true,
          fileName: "definitions/filename.sql",
          hermeticity: "HERMETIC",
          tags: ["tagA", "tagB"],
          query: "SELECT 1"
        }
      ])
    );
  });

  suite("Assertions as dependencies", ({ beforeEach }) => {
    [
      WorkflowSettingsTemplates.bigquery,
      WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
      WorkflowSettingsTemplates.bigqueryWithNamePrefix
    ].forEach(testConfig => {
      let projectDir: any;
      beforeEach("Create temporary dir and files", () => {
        projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(dataform.WorkflowSettings.create(testConfig))
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/A.sqlx"),
          `
config {
  type: "table",
  assertions: {rowConditions: ["test > 1"]}}
  SELECT 1 as test`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/A_assert.sqlx"),
          `
config {
  type: "assertion",
}
select test from \${ref("A")} where test > 3`
        );
        fs.writeFileSync(path.join(projectDir, "definitions/B.sql"), "SELECT 1");
        fs.writeFileSync(path.join(projectDir, "definitions/C.sql"), "SELECT 1");
        fs.writeFileSync(
          path.join(projectDir, `definitions/notebook.ipynb`),
          EMPTY_NOTEBOOK_CONTENTS
        );
      });

      test("When dependOnDependencyAssertions property is set to true, assertions from A are added as dependencies", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependOnDependencyAssertions: true,
  dependencies: ["A"]
}
select 1 as btest
`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert")
        ]);
      });

      test("Setting includeDependentAssertions to true in config.dependencies adds assertions from that dependency to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: true}, "C"]
}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
  assertions: {
    rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(4);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "C")
        ]);
      });

      test("Setting includeDependentAssertions to true in ref, adds assertions from that dependency to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "C"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_C_assertions_rowConditions")
        ]);
      });

      test("When dependOnDependencyAssertions=true and includeDependentAssertions=false, the assertions related to dependency should not be added to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependOnDependencyAssertions: true,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(4);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "C")
        ]);
      });

      test("When dependOnDependencyAssertions=false and includeDependentAssertions=true, the assertions related to dependency should be added to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "operations",
  dependOnDependencyAssertions: false,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations.find(
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations
              .find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              )
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "C"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_C_assertions_rowConditions")
        ]);
      });

      test("Assertions added through includeDependentAssertions and explicitly listed in dependencies are deduplicated.", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: ["A_assert"]
}
select * from \${ref({name: "A", includeDependentAssertions: true})}
select 1 as btest`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions")
        ]);
      });

      test("When includeDependentAssertions property in config and ref are set differently for the same dependency, compilation error is thrown.", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: false}, {name: "C", includeDependentAssertions: true}]
}
select * from \${ref({name: "A", includeDependentAssertions: true})}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test
}`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).deep.equals(2);
        expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).deep.equals(
          `Conflicting "includeDependentAssertions" properties are not allowed. Dependency A has different values set for this property.`
        );
      });

      suite("Action configs", () => {
        test(`When dependOnDependencyAssertions property is set to true, assertions from A are added as dependencies`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- operation:
    filename: C.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        });

        test(`Setting includeDependentAssertions to true in config.dependencies adds assertions from that dependency to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true 
- operation:
    filename: C.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
- notebook:
    filename: notebook.ipynb
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
        });

        test(`When dependOnDependencyAssertions=true and includeDependentAssertions=false, the assertions related to dependency should not be added to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
- assertion:
    filename: B_assert.sql
    dependencyTargets:
      - name: B
- operation:
    filename: C.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
      - name: B
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(1);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(1);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
        });

        test(`When dependOnDependencyAssertions=false and includeDependentAssertions=true, the assertions related to dependency should be added to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
- assertion:
    filename: B_assert.sql
    dependencyTargets:
      - name: B
- operation:
    filename: C.sql
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(4);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(4);
        });

        test(`When includeDependentAssertions property in config and ref are set differently for the same dependency, compilation error is thrown.`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- operation:
    filename: C.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
      - name: A
        includeDependentAssertions: false
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).deep.equals(1);
          expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).deep.equals(
            `Conflicting "includeDependentAssertions" properties are not allowed. Dependency A has different values set for this property.`
          );
        });
      });
    });
  });

  suite("disableAssertions", () => {
    [
      {
        testName: "sqlx file assertions",
        setupFiles: (projectDir: string) => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/assertion.sqlx"),
            `config { type: "assertion" }\nSELECT 1 WHERE FALSE`
          );
        }
      },
      {
        testName: "JavaScript API assertions",
        setupFiles: (projectDir: string) => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/assertion.js"),
            `assert("test_assertion").query("SELECT 1 WHERE FALSE");`
          );
        }
      },
      {
        testName: "YAML action config assertions",
        setupFiles: (projectDir: string) => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- assertion:
    name: yaml_assertion
    filename: assertion.sql`
          );
          fs.writeFileSync(
            path.join(projectDir, "definitions/assertion.sql"),
            "SELECT 1 WHERE FALSE"
          );
        }
      }
    ].forEach(testCase => {
      test(`disables ${testCase.testName} when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        
        testCase.setupFiles(projectDir);

        const coreRequest = coreExecutionRequestFromPath(projectDir);
        coreRequest.compile.compileConfig.disableAssertions = true;
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(result.compile.compiledGraph.assertions).deep.equals([]);
      });
    });

    [
      "table",
      "view",
      "incremental"
    ].forEach(tableType => {
      test(`disables inline ${tableType} assertions when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, `definitions/${tableType}.sqlx`),
          `config {
            type: "${tableType}",
            assertions: {
              uniqueKey: ["id"],
              nonNull: ["name"],
              rowConditions: ["id > 0"]
            }
          }
          SELECT 1 as id, 'test' as name`
        );

        const coreRequest = coreExecutionRequestFromPath(projectDir);
        coreRequest.compile.compileConfig.disableAssertions = true;
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(result.compile.compiledGraph.assertions).deep.equals([]);
        expect(result.compile.compiledGraph.tables.length).equals(1);
      });
    });
  });
});

function prefixAdjustedName(prefix: string | undefined, name: string) {
  return prefix ? `${prefix}_${name}` : name;
}
