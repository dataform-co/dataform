import { expect } from "chai";

import { exampleBuiltInAssertionsAsYaml } from "df/core/actions/index_test";
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

suite("table", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`tables can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- table:
    filename: action.sql`
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
            query: "SELECT 1",
            type: "table",
            enumType: "TABLE",
            disabled: false
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
- table:
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
    description: description
    partitionBy: partitionBy
    partitionExpirationDays: 1
    requirePartitionFilter: true
    clusterBy:
    - clusterBy
    labels:
        key: val
    additionalOptions:
        option1Key: option1
        option2Key: option2
    dependOnDependencyAssertions: true
    hermetic: true
    reservation: reservation
${exampleBuiltInAssertionsAsYaml.inputActionConfigBlock}
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
          fileName: "definitions/filename.sql",
          query: "SELECT 1",
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

  test("fails compilation if incrementalStrategy is set on standard table", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "table.sqlx",
      `config {
        type: "table",
        incrementalStrategy: "merge"
      }
      SELECT 1`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      'Unexpected property "incrementalStrategy"'
    );
  });

  suite("jit compilation", () => {
    test("jit compilation is supported", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "table.js",
        `publish("table", {type: "table"}).jitCode((ctx) => Promise.resolve({query: "select 1"}))`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "table"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "table"
          },
          type: "table",
          enumType: "TABLE",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/table.js",
          jitCode: '(ctx) => Promise.resolve({query: "select 1"})',
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
        "table.js",
        `publish("table", {type: "table"}).jitCode((ctx) => Promise.resolve({query: "select 1"})).query("select 1")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.some(e =>
          e.message.includes("Cannot mix AoT and JiT compilation")
        )
      ).equals(true);
    });
  });

  suite("reservation", () => {
    test("defaultReservation in workflow settings is applied to projectConfig", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultReservation: projects/my-project/locations/us/reservations/my-reservation
`
      );
      writeDefinitionFile(
        projectDir,
        "table.sqlx",
        `
config {
  type: "table"
}
SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals({
        defaultDatabase: "defaultProject",
        defaultSchema: "defaultDataset",
        defaultLocation: "US",
        defaultReservation: "projects/my-project/locations/us/reservations/my-reservation",
        warehouse: "bigquery"
      });
      // The action itself should have no actionDescriptor (no action-level reservation set).
      expect(asPlainObject(result.compile.compiledGraph.tables[0].actionDescriptor)).equals(null);
    });

    test("action-level reservation overrides the default reservation from workflow settings", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultReservation: projects/my-project/locations/us/reservations/default-reservation
`
      );
      writeDefinitionFile(
        projectDir,
        "table.sqlx",
        `
config {
  type: "table",
  reservation: "projects/my-project/locations/us/reservations/action-reservation"
}
SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      // The default reservation is available in projectConfig.
      expect(
        asPlainObject(result.compile.compiledGraph.projectConfig).defaultReservation
      ).deep.equals("projects/my-project/locations/us/reservations/default-reservation");
      // The action-level reservation is stored in actionDescriptor, taking precedence at runtime.
      expect(
        asPlainObject(result.compile.compiledGraph.tables[0].actionDescriptor)
      ).deep.equals({
        reservation: "projects/my-project/locations/us/reservations/action-reservation"
      });
    });
  });

  suite("shared config", () => {
    test("a shared config object can be reused across publish() calls without losing fields", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "shared.js",
        `
const shared = {
  type: "table",
  bigquery: { partitionBy: "event_date", clusterBy: ["user_id"] },
  assertions: { uniqueKey: "id", nonNull: "id" }
};
publish("t1", shared).query(_ => "SELECT 1 AS id, DATE '2024-01-01' AS event_date, 'u1' AS user_id");
publish("t2", shared).query(_ => "SELECT 2 AS id, DATE '2024-01-01' AS event_date, 'u2' AS user_id");
publish("t3", shared).query(_ => "SELECT 3 AS id, DATE '2024-01-01' AS event_date, 'u3' AS user_id");
`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);

      const bigqueryBlocks = result.compile.compiledGraph.tables.map(t =>
        asPlainObject(t.bigquery)
      );
      const expectedBigquery = { partitionBy: "event_date", clusterBy: ["user_id"] };
      expect(bigqueryBlocks).deep.equals([expectedBigquery, expectedBigquery, expectedBigquery]);

      const assertionNames = result.compile.compiledGraph.assertions
        .map(a => a.target.name)
        .sort();
      expect(assertionNames).deep.equals([
        "defaultDataset_t1_assertions_rowConditions",
        "defaultDataset_t1_assertions_uniqueKey_0",
        "defaultDataset_t2_assertions_rowConditions",
        "defaultDataset_t2_assertions_uniqueKey_0",
        "defaultDataset_t3_assertions_rowConditions",
        "defaultDataset_t3_assertions_uniqueKey_0"
      ]);
    });

    test("jit compilation is supported in sqlx", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/table.sqlx"),
        `config { type: "table", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "table"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "table"
          },
          type: "table",
          enumType: "TABLE",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/table.sqlx",
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

SELECT 1\`
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
        path.join(projectDir, "definitions/table.sqlx"),
        `config { type: "table", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1
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
            name: "table"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "table"
          },
          type: "table",
          enumType: "TABLE",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/table.sqlx",
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
});
