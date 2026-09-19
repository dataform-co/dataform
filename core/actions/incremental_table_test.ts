import { expect } from "chai";

import { exampleBuiltInAssertionsAsYaml } from "df/core/actions/index_test";
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

suite("incremental table", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`incremental tables can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- incrementalTable:
    filename: action.sql
    protected: true
    uniqueKey:
    -  someKey1
    -  someKey2`
      );
      writeDefinitionFile(projectDir, "action.sql", "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            fileName: "definitions/action.sql",
            hermeticity: "NON_HERMETIC",
            onSchemaChange: "IGNORE",
            query: "SELECT 1",
            incrementalQuery: "SELECT 1",
            incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
            type: "incremental",
            enumType: "INCREMENTAL",
            protected: true,
            disabled: false,
            uniqueKey: ["someKey1", "someKey2"]
          }
        ])
      );
    });

    test("action config options", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, "filename.sql", "SELECT 1");
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- incrementalTable:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
    - name: operation
    filename: filename.sql
    tags:
    - tag1
    - tag2
    disabled: true
    protected: true
    uniqueKey:
    - key1
    - key2
    description: description
    partitionBy: partitionBy
    partitionExpirationDays: 1
    requirePartitionFilter: true
    updatePartitionFilter: "updatePartitionFilter"
    clusterBy:
    - clusterBy
    labels:
        key: val
    additionalOptions:
        option1Key: "option1"
        option2Key: "option2"
    dependOnDependencyAssertions: true
${exampleBuiltInAssertionsAsYaml.inputActionConfigBlock}
    hermetic: true
    reservation: reservation
    onSchemaChange: FAIL
  `
      );

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
          type: "incremental",
          disabled: true,
          protected: true,
          hermeticity: "HERMETIC",
          onSchemaChange: "FAIL",
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
            requirePartitionFilter: true,
            updatePartitionFilter: "updatePartitionFilter"
          },
          tags: ["tag1", "tag2"],
          uniqueKey: ["key1", "key2"],
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "operation"
            }
          ],
          enumType: "INCREMENTAL",
          fileName: "definitions/filename.sql",
          query: "SELECT 1",
          incrementalQuery: "SELECT 1",
          incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
          actionDescriptor: {
            bigqueryLabels: {
              key: "val"
            },
            description: "description",
            reservation: "reservation"
          }
        }
      ]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        exampleBuiltInAssertionsAsYaml.outputAssertions
      );
    });
  });

  test("incremental table without defaultProject in workflow_settings", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    // Create workflow_settings without defaultProject (only defaultDataset and defaultLocation)
    writeWorkflowSettingsFile(
      projectDir,
      `defaultDataset: dataform
defaultLocation: europe-west2
`
    );
    writeDefinitionFile(
      projectDir,
      "incremental_table_without_default_project.sqlx",
      `config {
    type: "incremental",
    name: "incremental_table_without_default_project"
}

select \${incremental()} as is_incremental`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const compiledTable = result.compile.compiledGraph.tables[0];
    // Verify the table compiles without database field since defaultProject is not set.
    // The BigQuery adapter's table() method uses the input target (which doesn't have a database)
    // when defaultProject is not specified, ensuring that warehouse state targets match compiled targets.
    // This allows incremental appends to work correctly even when defaultProject is not in workflow_settings.yaml.
    expect(compiledTable.type).equals("incremental");
    expect(compiledTable.enumType).equals(dataform.TableType.INCREMENTAL);
    expect(compiledTable.target.schema).equals("dataform");
    expect(compiledTable.target.name).equals("incremental_table_without_default_project");
    expect(compiledTable.target.database).equals("");
  });

  suite("jit compilation", () => {
    test("jit compilation is supported", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "incremental.js",
        `publish("incremental", {type: "incremental"}).jitCode((ctx) => Promise.resolve({query: "select 1", incrementalQuery: "select 1"}))`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          type: "incremental",
          enumType: "INCREMENTAL",
          disabled: false,
          protected: false,
          hermeticity: "NON_HERMETIC",
          onSchemaChange: "IGNORE",
          fileName: "definitions/incremental.js",
          incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
          jitCode: '(ctx) => Promise.resolve({query: "select 1", incrementalQuery: "select 1"})',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        }
      ]);
    });

    test("jit compilation fails if query is also provided", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "incremental.js",
        `publish("incremental", {type: "incremental"}).jitCode((ctx) => ({query: "select 1", incrementalQuery: "select 1"})).query("select 1")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors.some(e => e.message.includes("Cannot mix AoT and JiT compilation"))).equals(true);
    });

    test("jit compilation is supported in sqlx", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/incremental.sqlx"),
        `config { type: "incremental", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1
\${when(incremental(), "WHERE 1=1")}`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          type: "incremental",
          enumType: "INCREMENTAL",
          disabled: false,
          protected: false,
          hermeticity: "NON_HERMETIC",
          onSchemaChange: "IGNORE",
          fileName: "definitions/incremental.sqlx",
          jitCode: `async (jctx) => {
    const self = jctx.self ? jctx.self.bind(jctx) : undefined;
    const ref = jctx.ref ? jctx.ref.bind(jctx) : undefined;
    const resolve = jctx.resolve ? jctx.resolve.bind(jctx) : undefined;
    const name = jctx.name ? jctx.name.bind(jctx) : undefined;
    const when = jctx.when ? jctx.when.bind(jctx) : undefined;
    const incremental = jctx.incremental ? jctx.incremental.bind(jctx) : undefined;
    const schema = jctx.schema ? jctx.schema.bind(jctx) : undefined;
    const database = jctx.database ? jctx.database.bind(jctx) : undefined;
    const adapter = jctx.adapter ? jctx.adapter : undefined;
    const data = jctx.data ? jctx.data : undefined;
    
  const foo = "bar";

    return {
        query: (
          \`

SELECT 1
\${when(incremental(), "WHERE 1=1")}\`
        ),
        postOps: (
          undefined
        ),
        preOps: (
          undefined
        ),
      };
    }`,
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        }
      ]);
    });

    test("jit compilation with pre/post ops is supported in sqlx", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/incremental.sqlx"),
        `config { type: "incremental", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1
\${when(incremental(), "WHERE 1=1")};
pre_operations {
  SELECT 10;
}
post_operations {
  SELECT 20;
}`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "incremental"
          },
          type: "incremental",
          enumType: "INCREMENTAL",
          disabled: false,
          protected: false,
          hermeticity: "NON_HERMETIC",
          onSchemaChange: "IGNORE",
          fileName: "definitions/incremental.sqlx",
          jitCode: `async (jctx) => {
    const self = jctx.self ? jctx.self.bind(jctx) : undefined;
    const ref = jctx.ref ? jctx.ref.bind(jctx) : undefined;
    const resolve = jctx.resolve ? jctx.resolve.bind(jctx) : undefined;
    const name = jctx.name ? jctx.name.bind(jctx) : undefined;
    const when = jctx.when ? jctx.when.bind(jctx) : undefined;
    const incremental = jctx.incremental ? jctx.incremental.bind(jctx) : undefined;
    const schema = jctx.schema ? jctx.schema.bind(jctx) : undefined;
    const database = jctx.database ? jctx.database.bind(jctx) : undefined;
    const adapter = jctx.adapter ? jctx.adapter : undefined;
    const data = jctx.data ? jctx.data : undefined;
    
  const foo = "bar";

    return {
        query: (
          \`

SELECT 1
\${when(incremental(), "WHERE 1=1")};

\`
        ),
        postOps: (
          [\`
  SELECT 20;
\`]
        ),
        preOps: (
          [\`
  SELECT 10;
\`]
        ),
      };
    }`,
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        }
      ]);
    });
  });

  suite("incrementalStrategy", () => {
    test("compiles successfully with insert_overwrite and partitionBy", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          incrementalStrategy: "insert_overwrite",
          partitionBy: "DATE(ts)"
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "incremental"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "incremental"
            },
            fileName: "definitions/incremental.sqlx",
            hermeticity: "NON_HERMETIC",
            onSchemaChange: "IGNORE",
            query: "\n        SELECT 1",
            incrementalQuery: "\n        SELECT 1",
            incrementalStrategy: "INSERT_OVERWRITE",
            type: "incremental",
            enumType: "INCREMENTAL",
            protected: false,
            disabled: false,
            bigquery: {
              partitionBy: "DATE(ts)"
            }
          }
        ])
      );
    });

    test("compilation fails with insert_overwrite and missing partitionBy", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          incrementalStrategy: "insert_overwrite"
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "IncrementalStrategy 'insert_overwrite' requires 'partitionBy' to be set"
      );
    });

    test("compiles successfully with merge and uniqueKey", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          incrementalStrategy: "merge",
          uniqueKey: ["id"]
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "incremental"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "incremental"
            },
            fileName: "definitions/incremental.sqlx",
            hermeticity: "NON_HERMETIC",
            onSchemaChange: "IGNORE",
            query: "\n        SELECT 1",
            incrementalQuery: "\n        SELECT 1",
            incrementalStrategy: "MERGE",
            type: "incremental",
            enumType: "INCREMENTAL",
            protected: false,
            disabled: false,
            uniqueKey: ["id"]
          }
        ])
      );
    });

    test("compilation fails with merge and missing uniqueKey", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          incrementalStrategy: "merge"
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "IncrementalStrategy 'merge' requires 'uniqueKey' to be set"
      );
    });

    test("compilation fails with invalid incrementalStrategy", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          incrementalStrategy: "invalid_strategy"
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        'IncrementalStrategy value "invalid_strategy" is not supported'
      );
    });

    test("compilation fails when both incrementalPredicates and updatePartitionFilter are provided", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        VALID_WORKFLOW_SETTINGS_YAML
      );
      writeDefinitionFile(
        projectDir,
        "incremental.sqlx",
        `config {
          type: "incremental",
          bigquery: {
            updatePartitionFilter: "T.foo = 1",
            incrementalPredicates: ["bar = 2"]
          }
        }
        SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "incrementalPredicates and updatePartitionFilter cannot be both set. Use only incrementalPredicates."
      );
    });
  });
});
