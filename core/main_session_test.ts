import { expect } from "chai";

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
  WorkflowSettingsTemplates,
} from "df/testing/run_core";

interface IVerifiableAction {
  type?: string | null;
  target?: dataform.ITarget | null;
  canonicalTarget?: dataform.ITarget | null;
  dependencyTargets?: dataform.ITarget[] | null;
}

function toVerifiableAction(graph: dataform.ICompiledGraph, actionType: string): IVerifiableAction {
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
    dependencyTargets: action.dependencyTargets,
  };
}

suite("session", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  suite("resolve succeeds", () => {
    [
      WorkflowSettingsTemplates.bigquery,
      WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
      WorkflowSettingsTemplates.bigqueryWithNamePrefix,
    ].forEach((testConfig) => {
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
          `\`defaultDataset${suffix}.${prefix}e\``,
        );
      });
    });
  });

  suite("resolve with legacy dependencies", () => {
    ["assertion", "incremental", "operations", "table", "view"].forEach((actionType) => {
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

SELECT 1`,
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

SELECT 1`,
        );
        writeDefinitionFile(
          projectDir,
          "file_with_dependencies.sqlx",
          `
config {
type: "${actionType}",
dependencies: ["schema_a.tbl"]
}

SELECT 1`,
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
              name: "file_with_dependencies",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "file_with_dependencies",
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "schema_a",
                name: "tbl",
              },
            ],
          }),
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
      asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors[0].message),
    ).deep.equals(`Could not resolve "e"`);
  });

  test("failed ref attributes the compilation error to the source sqlx file", () => {
    // Errors from Session.resolve must be attributed to the action's source
    // file, not to the entry point ("index.js").
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "mytable.sqlx",
      `config { type: "view" }\nSELECT 1 FROM \${ref("FOO")}`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    const errors = result.compile.compiledGraph.graphErrors.compilationErrors;
    const resolveError = errors.find((e) => e.message === `Could not resolve "FOO"`);
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
publish("b", {"schema": "foo"}).dependencies("a")`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map((error) => error.message),
    ).deep.equals([
      `Ambiguous Action name: {\"name\":\"a\",\"includeDependentAssertions\":false}. Did you mean one of: foo.a, bar.a.`,
    ]);
  });

  suite("context methods", () => {
    [
      WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
      {
        ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
        defaultProject: "defaultProject",
      },
      { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" },
    ].forEach((testConfig) => {
      test(
        `assertions target context functions with project suffix '${testConfig.projectSuffix}', ` +
          `dataset suffix '${testConfig.datasetSuffix}', and name prefix '${testConfig.namePrefix}'`,
        () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          writeWorkflowSettingsFile(projectDir, testConfig);
          writeDefinitionFile(
            projectDir,
            "file.js",
            'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)',
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(
            asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors),
          ).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.assertions[0].query)).deep.equals(
            `defaultProject${testConfig.projectSuffix ? `_suffix` : ""}.` +
              `defaultDataset${testConfig.datasetSuffix ? `_suffix` : ""}.` +
              `${testConfig.namePrefix ? `prefix_` : ""}name`,
          );
        },
      );
    });

    test("assertions database function fails when database is undefined on the proto", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
      writeDefinitionFile(
        projectDir,
        "file.js",
        'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)',
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors?.[0]?.message),
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
    filename: table2.extradot.sql`,
    );
    writeDefinitionFile(projectDir, "table2.extradot.sql", "SELECT 2");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors
        .map(({ message }) => message)
        .sort(),
    ).deep.equals([
      `Action target datasets cannot include '.'`,
      `Action target datasets cannot include '.'`,
      `Action target names cannot include '.'`,
      `Action target names cannot include '.'`,
      `Action target names cannot include '.'`,
    ]);
  });

  [
    { filename: "action.sqlx", contents: `config { type: "table" } SELECT 1` },
    { filename: "action.js", contents: `publish("action", { type: "table" }).query("SELECT 1")` },
    {
      filename: "operation.sqlx",
      contents: `config { type: "operations", name: "action", hasOutput: true } SELECT 1`,
    },
    {
      filename: "operation.js",
      contents: `operate("action", { hasOutput: true }).queries("SELECT 1")`,
    },
    {
      filename: "graph.yaml",
      contents: `
version: 0.2.0.dev0/google
semantic_model:
- name: action
  entities:
  - name: Node
    source: bigquery:defaultProject.raw.nodes
    primary_key:
    - id
`,
    },
  ].forEach(({ filename, contents }) => {
    test(`fails when dataset is not set in workflow settings or the action in ${filename}`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
defaultProject: defaultProject
defaultLocation: US
`,
      );
      writeDefinitionFile(projectDir, filename, contents);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.map(
          ({ message, fileName, actionName, actionTarget }) => ({
            message,
            fileName,
            actionName,
            actionTarget: asPlainObject(actionTarget),
          }),
        ),
      ).deep.equals([
        {
          message:
            "Action targets must have a dataset. Set defaultDataset in workflow_settings.yaml or dataset in the action config.",
          fileName: `definitions/${filename}`,
          actionName: "defaultProject..action",
          actionTarget: { database: "defaultProject", name: "action" },
        },
      ]);
    });
  });

  test("fails when dataset is not set for an operation with hasOutput in actions.yaml", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultLocation: US
`,
    );
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- operation:
    filename: action.sql
    hasOutput: true
`,
    );
    writeDefinitionFile(projectDir, "action.sql", "SELECT 1");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors.map(
        ({ message, fileName, actionName, actionTarget }) => ({
          message,
          fileName,
          actionName,
          actionTarget: asPlainObject(actionTarget),
        }),
      ),
    ).deep.equals([
      {
        message:
          "Action targets must have a dataset. Set defaultDataset in workflow_settings.yaml or dataset in the action config.",
        fileName: "definitions/action.sql",
        actionName: "defaultProject..action",
        actionTarget: { database: "defaultProject", name: "action" },
      },
    ]);
  });

  test("succeeds when dataset is not set for operations without output", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, `dataformCoreVersion: ${version}`);
    writeDefinitionFile(projectDir, "../staged.sql", "SELECT 1");
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- operation:
    filename: ../staged.sql
`,
    );
    writeDefinitionFile(
      projectDir,
      "sqlx_op.sqlx",
      `config { type: "operations", database: "opProject" } SELECT 2`,
    );
    writeDefinitionFile(projectDir, "js_op.js", `operate("js_op").queries("SELECT 3")`);

    const result = runMainInVm(
      coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({
          databaseSuffix: "db",
          schemaSuffix: "ds",
          tablePrefix: "pre",
        }),
      ),
    );

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals([
      {
        target: { name: "pre_staged" },
        canonicalTarget: { name: "staged" },
        fileName: "staged.sql",
        hermeticity: "NON_HERMETIC",
        queries: ["SELECT 1"],
      },
      {
        target: { name: "pre_js_op" },
        canonicalTarget: { name: "js_op" },
        fileName: "definitions/js_op.js",
        hermeticity: "NON_HERMETIC",
        queries: ["SELECT 3"],
      },
      {
        target: { database: "opProject_db", name: "pre_sqlx_op" },
        canonicalTarget: { database: "opProject", name: "sqlx_op" },
        fileName: "definitions/sqlx_op.sqlx",
        hermeticity: "NON_HERMETIC",
        queries: [" SELECT 2"],
      },
    ]);
    expect(asPlainObject(result.compile.compiledGraph.targets)).deep.equals([
      { name: "pre_staged" },
      { name: "pre_js_op" },
      { database: "opProject_db", name: "pre_sqlx_op" },
    ]);
  });

  test("fails when schema() is called in a non-output operation without a dataset", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultLocation: US
`,
    );
    writeDefinitionFile(
      projectDir,
      "sqlx_op.sqlx",
      `config { type: "operations" } SELECT * FROM \`\${schema()}.tbl\``,
    );
    writeDefinitionFile(
      projectDir,
      "js_op.js",
      `operate("js_op").queries(ctx => \`SELECT * FROM \\\`\${ctx.schema()}.tbl\\\`\`)`,
    );

    const result = runMainInVm(
      coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({ schemaSuffix: "ds" }),
      ),
    );

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors.map(
        ({ message, fileName, actionName, actionTarget }) => ({
          message,
          fileName,
          actionName,
          actionTarget: asPlainObject(actionTarget),
        }),
      ),
    ).deep.equals([
      {
        message:
          "Action targets must have a dataset to call schema(). Set defaultDataset in workflow_settings.yaml or dataset in the action config.",
        fileName: "definitions/js_op.js",
        actionName: "defaultProject..js_op",
        actionTarget: { database: "defaultProject", name: "js_op" },
      },
      {
        message:
          "Action targets must have a dataset to call schema(). Set defaultDataset in workflow_settings.yaml or dataset in the action config.",
        fileName: "definitions/sqlx_op.sqlx",
        actionName: "defaultProject..sqlx_op",
        actionTarget: { database: "defaultProject", name: "sqlx_op" },
      },
    ]);
    expect(result.compile.compiledGraph.operations.map((op) => op.queries)).deep.equals([
      ["SELECT * FROM `.tbl`"],
      [" SELECT * FROM `.tbl`"],
    ]);
  });

  test("backslash file paths in the compile config are normalized to forward slashes", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "a.sqlx", `config { type: "table" } SELECT 1 AS test`);
    const coreExecutionRequest = dataform.CoreExecutionRequest.create({
      compile: {
        compileConfig: {
          projectDir,
          filePaths: ["definitions\\a.sqlx"],
        },
      },
    });

    const result = runMainInVm(coreExecutionRequest);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables.map((table) => table.fileName)).deep.equals([
      "definitions/a.sqlx",
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
publish("name")`,
    );

    const result = runMainInVm(
      coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({
          defaultSchema: "otherDataset",
        }),
      ),
    );

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map((error) => error.message),
    ).deep.equals([
      `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
      `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
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
publish("b").dependencies("a")`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map((error) => error.message),
    ).deep.equals([
      `Circular dependency detected in chain: [{\"database\":\"defaultProject\",\"name\":\"a\",\"schema\":\"defaultDataset\"} > {\"database\":\"defaultProject\",\"name\":\"b\",\"schema\":\"defaultDataset\"} > defaultProject.defaultDataset.a]`,
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
publish("a").dependencies("b")`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map((error) => error.message),
    ).deep.equals([
      `Missing dependency detected: Action \"defaultProject.defaultDataset.a\" depends on \"{\"name\":\"b\",\"includeDependentAssertions\":false}\" which does not exist`,
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
publish("b", "SELECT 1;");`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(
      result.compile.compiledGraph.graphErrors.compilationErrors?.map((error) => error.message),
    ).deep.equals([
      "Semi-colons are not allowed at the end of SQL statements.",
      "Semi-colons are not allowed at the end of SQL statements.",
    ]);
  });

  suite("compiledGraph targets", () => {
    test("applies schemaSuffix, databaseSuffix, and tablePrefix to non-declaration targets", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultAssertionDataset: defaultAssertions
defaultLocation: US
`,
      );
      writeDefinitionFile(
        projectDir,
        "source.sqlx",
        `config { type: "declaration", name: "ext_source" }`,
      );
      writeDefinitionFile(
        projectDir,
        "tbl.sqlx",
        `config { type: "table", assertions: { nonNull: ["id"] } }\nSELECT 1 AS id`,
      );
      writeDefinitionFile(
        projectDir,
        "op.sqlx",
        `config { type: "operations", hasOutput: true }\nSELECT 1`,
      );
      writeDefinitionFile(
        projectDir,
        "lakehouse_tbl.sqlx",
        `config { type: "table", lakehouse: { catalog: "my_catalog", namespace: "my_namespace" } }\nSELECT 1 AS id`,
      );
      writeDefinitionFile(
        projectDir,
        "graph.yaml",
        `
version: 0.2.0.dev0/google
semantic_model:
- name: MyGraph
  entities:
  - name: Item
    source:
      ref: tbl
    primary_key:
    - id
`,
      );

      const result = runMainInVm(
        coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            databaseSuffix: "db",
            schemaSuffix: "ds",
            tablePrefix: "pre",
          }),
        ),
      );

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.targets)).deep.equals([
        {
          database: "defaultProject_db",
          schema: "defaultDataset_ds",
          name: "pre_MyGraph",
        },
        {
          database: "defaultProject_db",
          schema: "my_catalog.my_namespace",
          name: "pre_lakehouse_tbl",
        },
        {
          database: "defaultProject_db",
          schema: "defaultDataset_ds",
          name: "pre_op",
        },
        {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "ext_source",
        },
        {
          database: "defaultProject_db",
          schema: "defaultAssertions_ds",
          name: "pre_defaultDataset_tbl_assertions_rowConditions",
        },
        {
          database: "defaultProject_db",
          schema: "defaultDataset_ds",
          name: "pre_tbl",
        },
      ]);
    });

    test("keeps declaration un-suffixed when colliding with a table's un-suffixed target", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.js",
        `
declare({ name: "shared" });
publish("shared", { type: "table" }).query("SELECT 1");
`,
      );

      const result = runMainInVm(
        coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            schemaSuffix: "dev",
          }),
        ),
      );

      expect(asPlainObject(result.compile.compiledGraph.targets)).deep.equals([
        {
          database: "defaultProject",
          schema: "defaultDataset",
          name: "shared",
        },
        {
          database: "defaultProject",
          schema: "defaultDataset_dev",
          name: "shared",
        },
      ]);
    });

    test("applies suffixes and prefixes to test targets", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "tbl.sqlx", `config { type: "table" }\nSELECT 1 AS id`);
      writeDefinitionFile(
        projectDir,
        "tbl_test.sqlx",
        `config { type: "test", dataset: "tbl" }\nSELECT 1 AS id`,
      );

      const result = runMainInVm(
        coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            databaseSuffix: "db",
            schemaSuffix: "dev",
            tablePrefix: "pre",
          }),
        ),
      );

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.targets)).deep.equals([
        {
          database: "defaultProject_db",
          schema: "defaultDataset_dev",
          name: "pre_tbl",
        },
        {
          database: "defaultProject_db",
          schema: "defaultDataset_dev",
          name: "pre_tbl_test",
        },
      ]);
    });
  });
});
