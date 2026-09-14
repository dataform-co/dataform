import { expect } from "chai";
import { dump as dumpYaml } from "js-yaml";

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
  WorkflowSettingsTemplates
} from "df/testing/run_core";

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("Assertions as dependencies", ({ afterEach, beforeEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  [
    WorkflowSettingsTemplates.bigquery,
    WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
    WorkflowSettingsTemplates.bigqueryWithNamePrefix
  ].forEach(testConfig => {
    let projectDir: any;
    beforeEach("Create temporary dir and files", () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, dumpYaml(dataform.WorkflowSettings.create(testConfig)));
      writeDefinitionFile(
        projectDir,
        "A.sqlx",
        `
config {
  type: "table",
  assertions: {rowConditions: ["test > 1"]}}
  SELECT 1 as test`
      );
      writeDefinitionFile(
        projectDir,
        "A_assert.sqlx",
        `
config {
  type: "assertion",
}
select test from \${ref("A")} where test > 3`
      );
      writeDefinitionFile(projectDir, "B.sql", "SELECT 1");
      writeDefinitionFile(projectDir, "C.sql", "SELECT 1");
      writeDefinitionFile(projectDir, "notebook.ipynb", EMPTY_NOTEBOOK_CONTENTS);
    });

    test("When dependOnDependencyAssertions property is set to true, assertions from A are added as dependencies", () => {
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
        `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: true}, "C"]
}
select 1 as btest`
      );
      writeDefinitionFile(
        projectDir,
        "C.sqlx",
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
        `
config {
  type: "table",
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
      );
      writeDefinitionFile(
        projectDir,
        "C.sqlx",
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
        `
config {
  type: "table",
  dependOnDependencyAssertions: true,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
      );
      writeDefinitionFile(
        projectDir,
        "C.sqlx",
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
        `
config {
  type: "operations",
  dependOnDependencyAssertions: false,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
      );
      writeDefinitionFile(
        projectDir,
        "C.sqlx",
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
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
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
      writeDefinitionFile(
        projectDir,
        "B.sqlx",
        `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: false}, {name: "C", includeDependentAssertions: true}]
}
select * from \${ref({name: "A", includeDependentAssertions: true})}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
      );
      writeDefinitionFile(
        projectDir,
        "C.sqlx",
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
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
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
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
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
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
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
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
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
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
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
        writeDefinitionFile(projectDir, "B_assert.sql", "SELECT test from B");

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations.find(
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
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
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
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
        writeDefinitionFile(projectDir, "B_assert.sql", "SELECT test from B");

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations.find(
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
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
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
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
        writeDefinitionFile(projectDir, "B_assert.sql", "SELECT test from B");

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).deep.equals(1);
        expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).deep.equals(
          `Conflicting "includeDependentAssertions" properties are not allowed. Dependency A has different values set for this property.`
        );
      });
    });
  });
});

function prefixAdjustedName(prefix: string | undefined, name: string) {
  return prefix ? `${prefix}_${name}` : name;
}
