import { expect } from "chai";

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
  VALID_WORKFLOW_SETTINGS_YAML,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

interface IVerifiableAction {
  type?: string | null;
  target?: dataform.ITarget | null;
  canonicalTarget?: dataform.ITarget | null;
  dependencyTargets?: dataform.ITarget[] | null;
}

function toVerifiableAction(
  graph: dataform.ICompiledGraph,
  actionType: string
): IVerifiableAction {
  let action: dataform.IAssertion | dataform.ITable | dataform.IPropertyGraph;
  switch (actionType) {
    case "assertion":
      action = graph.assertions[0];
      break;
    case "operations":
      action = graph.operations[0];
      break;
    case "propertyGraph":
      action = graph.propertyGraphs[0];
      break;
    default:
      action = graph.tables[0];
  }

  return {
    type: actionType,
    target: action.target,
    canonicalTarget: action.canonicalTarget,
    dependencyTargets: action.dependencyTargets
  };
}

suite("session", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  suite("resolve succeeds", () => {
    [
      WorkflowSettingsTemplates.bigquery,
      WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
      WorkflowSettingsTemplates.bigqueryWithNamePrefix
    ].forEach(testConfig => {
      test(`resolve with name prefix "${testConfig.namePrefix}" and dataset suffix "${testConfig.datasetSuffix}"`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, testConfig);
        writeDefinitionFile(projectDir, "e.sqlx", `config {type: "view"}`);
        writeDefinitionFile(projectDir, "file.sqlx", "${resolve('e')}");

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        const suffix = testConfig.datasetSuffix ? `_${testConfig.datasetSuffix}` : "";
        const prefix = testConfig.namePrefix ? `${testConfig.namePrefix}_` : "";
        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(result.compile.compiledGraph.operations[0].queries[0]).deep.equals(
          `\`defaultDataset${suffix}.${prefix}e\``
        );
      });
    });
  });

  suite("resolve with legacy dependencies", () => {
    ["assertion", "incremental", "operations", "table", "view"].forEach(actionType => {
      test(`for action type: "${actionType}"`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "source_a.sqlx",
          `
config {
type: "table",
schema: "schema_a",
name: "tbl",
}

SELECT 1`
        );
        writeDefinitionFile(
          projectDir,
          "source_b.sqlx",
          `
config {
type: "table",
schema: "schema_b",
name: "tbl",
}

SELECT 1`
        );
        writeDefinitionFile(
          projectDir,
          "file_with_dependencies.sqlx",
          `
config {
type: "${actionType}",
dependencies: ["schema_a.tbl"]
}

SELECT 1`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        const compiledAction = toVerifiableAction(result.compile.compiledGraph, actionType);
        expect(asPlainObject(compiledAction)).deep.equals(
          asPlainObject({
            type: actionType,
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "file_with_dependencies"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "file_with_dependencies"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "schema_a",
                name: "tbl"
              }
            ]
          })
        );
      });
    });
  });

  test("fails when cannot resolve", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "file.sqlx", "${resolve('e')}");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(asPlainObject(result.compile.compiledGraph.operations[0].queries[0])).deep.equals(``);
    expect(
      asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors[0].message)
    ).deep.equals(`Could not resolve "e"`);
  });

  test("failed ref attributes the compilation error to the source sqlx file", () => {
    // Regression test: after the vm2 3.11.3 upgrade, V8 CallSite objects
    // inside the sandbox lose their file paths, so utils.getCallerFile
    // cannot recover the caller during a callback invocation. Errors from
    // Session.resolve were therefore attributed to the sandbox entry
    // ("index.js") instead of the action's source file. This test locks
    // the fileName on the emitted CompilationError to the sqlx file.
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "mytable.sqlx",
      `config { type: "view" }\nSELECT 1 FROM \${ref("FOO")}`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    const errors = result.compile.compiledGraph.graphErrors.compilationErrors;
    const resolveError = errors.find(e => e.message === `Could not resolve "FOO"`);
    expect(resolveError, "expected a 'Could not resolve' error").to.not.equal(undefined);
    expect(resolveError.fileName).equals("definitions/mytable.sqlx");
  });

  test("fails when ambiguous resolve", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "file.js",
      `
publish("a", {"schema": "foo"})
publish("a", {"schema": "bar"})
publish("b", {"schema": "foo"}).dependencies("a")`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
    ).deep.equals([
      `Ambiguous Action name: {\"name\":\"a\",\"includeDependentAssertions\":false}. Did you mean one of: foo.a, bar.a.`
    ]);
  });

  suite("context methods", () => {
    [
      WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
      {
        ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
        defaultProject: "defaultProject"
      },
      { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
    ].forEach(testConfig => {
      test(
        `assertions target context functions with project suffix '${testConfig.projectSuffix}', ` +
          `dataset suffix '${testConfig.datasetSuffix}', and name prefix '${testConfig.namePrefix}'`,
        () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(projectDir, testConfig);
          writeDefinitionFile(
            projectDir,
            "file.js",
            'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)'
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(
            asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)
          ).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.assertions[0].query)).deep.equals(
            `defaultProject${testConfig.projectSuffix ? `_suffix` : ""}.` +
              `defaultDataset${testConfig.datasetSuffix ? `_suffix` : ""}.` +
              `${testConfig.namePrefix ? `prefix_` : ""}name`
          );
        }
      );
    });

    test("assertions database function fails when database is undefined on the proto", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
      writeDefinitionFile(
        projectDir,
        "file.js",
        'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)'
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors?.[0]?.message)
      ).deep.equals("Warehouse does not support multiple databases");
    });
  });

  test("filenames with multiple dots cause compilation errors", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "table1.extradot.sqlx", "SELECT 1");
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- operation:
    dataset: "dataset.extradot"
    filename: table2.extradot.sql`
    );
    writeDefinitionFile(projectDir, "table2.extradot.sql", "SELECT 2");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors
        .map(({ message }) => message)
        .sort()
    ).deep.equals([
      `Action target datasets cannot include '.'`,
      `Action target datasets cannot include '.'`,
      `Action target names cannot include '.'`,
      `Action target names cannot include '.'`,
      `Action target names cannot include '.'`
    ]);
  });

  test("fails when non-unique target", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "file.js",
      `
publish("name")
publish("name")`
    );

    const result = runMainInVm(
      coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({
          defaultSchema: "otherDataset"
        })
      )
    );

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
    ).deep.equals([
      `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`
    ]);
  });

  test("fails when circular dependencies", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "file.js",
      `
publish("a").dependencies("b")
publish("b").dependencies("a")`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
    ).deep.equals([
      `Circular dependency detected in chain: [{\"database\":\"defaultProject\",\"name\":\"a\",\"schema\":\"defaultDataset\"} > {\"database\":\"defaultProject\",\"name\":\"b\",\"schema\":\"defaultDataset\"} > defaultProject.defaultDataset.a]`
    ]);
  });

  test("fails when missing dependency", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "file.sql", "unused");
    writeDefinitionFile(
      projectDir,
      "file.js",
      `
publish("a").dependencies("b")`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
    ).deep.equals([
      `Missing dependency detected: Action \"defaultProject.defaultDataset.a\" depends on \"{\"name\":\"b\",\"includeDependentAssertions\":false}\" which does not exist`
    ]);
  });

  test("semi-colons at the end of SQL statements throws", () => {
    // If this didn't happen, then the generated SQL could be incorrect
    // because of being broken up by semi-colons.

    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "file.js",
      `
publish("a", "SELECT 1;\\n");
publish("b", "SELECT 1;");`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
    ).deep.equals([
      "Semi-colons are not allowed at the end of SQL statements.",
      "Semi-colons are not allowed at the end of SQL statements."
    ]);
  });
});
