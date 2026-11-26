import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { DataPreparation } from "df/core/actions/data_preparation";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

suite("data preparation", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("data preparations", () => {
    const createSimpleDataPreparationProject = (
      workflowSettingsYaml = VALID_WORKFLOW_SETTINGS_YAML,
      writeActionsYaml = true,
    ): string => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), workflowSettingsYaml);
      fs.mkdirSync(path.join(projectDir, "definitions"));

      if (writeActionsYaml) {
        fs.writeFileSync(
          path.join(projectDir, "definitions/actions.yaml"),
          `
actions:
- dataPreparation:
    filename: data_preparation.dp.yaml`,
        );
      }
      return projectDir;
    };

    test(`empty data preparation returns a default target`, () => {
      const projectDir = createSimpleDataPreparationProject();
      const dataPreparationYaml = `
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.dp.yaml"),
        dataPreparationYaml,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "data_preparation",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "data_preparation",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "data_preparation",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "data_preparation",
              },
            ],
            fileName: "definitions/data_preparation.dp.yaml",
            dataPreparationYaml: "",
          },
        ]),
      );
    });

    test(`data preparation with no targets a default target`, () => {
      const projectDir = createSimpleDataPreparationProject();
      const dataPreparationYaml = `
nodes:
- id: node1
  source:
    table:
      project: prj
      dataset: ds
      table: src
  generated:
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
    sourceGenerated:
      sourceSchema:
        tableSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.dp.yaml"),
        dataPreparationYaml,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "data_preparation",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "data_preparation",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "data_preparation",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "data_preparation",
              },
            ],
            fileName: "definitions/data_preparation.dp.yaml",
            dataPreparationYaml: dumpYaml(loadYaml(dataPreparationYaml)),
          },
        ]),
      );
    });

    test(`data preparations can be loaded via sqlx file`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    name: "dest",
    dataset: "ds",
    project: "prj",
    errorTable: {
        name: "errorTable",
        dataset: "errorDs",
        project: "errorPrj",
    }
}

FROM x
-- Ensure y is positive
|> $\{EXPECT\} y > 0
$\{when(true, "|> SELECT *", "|> SELECT 1")\}
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.sqlx"),
        dataPreparationSqlx,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "prj",
              schema: "ds",
              name: "dest",
            },
            canonicalTarget: {
              database: "prj",
              schema: "ds",
              name: "dest",
            },
            targets: [
              {
                database: "prj",
                schema: "ds",
                name: "dest",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "prj",
                schema: "ds",
                name: "dest",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            fileName: "definitions/data_preparation.sqlx",
            load: {
              replace: {},
            },
            query: `FROM x
-- Ensure y is positive
|>  /* @@VALIDATION */ WHERE  y > 0
|> SELECT *`,
            errorTable: {
              database: "errorPrj",
              schema: "errorDs",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparations can be loaded via sqlx file with file name as action name`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    dataset: "ds",
    project: "prj",
    errorTable: {
        name: "errorTable",
        dataset: "errorDs",
        project: "errorPrj",
    }
}

FROM x
|> $\{EXPECT\} y > 0
$\{when(true, "|> SELECT *", "|> SELECT 1")\}
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/this_is_the_file_name.sqlx"),
        dataPreparationSqlx,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "prj",
              schema: "ds",
              name: "this_is_the_file_name",
            },
            canonicalTarget: {
              database: "prj",
              schema: "ds",
              name: "this_is_the_file_name",
            },
            targets: [
              {
                database: "prj",
                schema: "ds",
                name: "this_is_the_file_name",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "prj",
                schema: "ds",
                name: "this_is_the_file_name",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            fileName: "definitions/this_is_the_file_name.sqlx",
            load: {
              replace: {},
            },
            query: `FROM x
|>  /* @@VALIDATION */ WHERE  y > 0
|> SELECT *`,
            errorTable: {
              database: "errorPrj",
              schema: "errorDs",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparations can be loaded via dp.sqlx file with file name as action name`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    dataset: "ds",
    project: "prj",
    errorTable: {
        name: "errorTable",
        dataset: "errorDs",
        project: "errorPrj",
    }
}

FROM x
|> $\{EXPECT\} y > 0
$\{when(true, "|> SELECT *", "|> SELECT 1")\}
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/this_is_the_file_name.dp.sqlx"),
        dataPreparationSqlx,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "prj",
              schema: "ds",
              name: "this_is_the_file_name",
            },
            canonicalTarget: {
              database: "prj",
              schema: "ds",
              name: "this_is_the_file_name",
            },
            targets: [
              {
                database: "prj",
                schema: "ds",
                name: "this_is_the_file_name",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "prj",
                schema: "ds",
                name: "this_is_the_file_name",
              },
              {
                database: "errorPrj",
                schema: "errorDs",
                name: "errorTable",
              },
            ],
            fileName: "definitions/this_is_the_file_name.dp.sqlx",
            load: {
              replace: {},
            },
            query: `FROM x
|>  /* @@VALIDATION */ WHERE  y > 0
|> SELECT *`,
            errorTable: {
              database: "errorPrj",
              schema: "errorDs",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparations can be loaded via sqlx file with compilation overrides`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    dataset: "compilation_override_dataset",
    project: "compilation_override_project",
    name: "dest",
    errorTable: {
        name: "errorTable",
    },
    loadMode: {
        mode: "APPEND",
    },
}

FROM x
|> SELECT *
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.sqlx"),
        dataPreparationSqlx,
      );

      const coreExecutionRequest = coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({
          defaultDatabase: "projectOverride",
          defaultSchema: "datasetOverride",
        }),
      );

      const result = runMainInVm(coreExecutionRequest);

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "compilation_override_project",
              schema: "compilation_override_dataset",
              name: "dest",
            },
            canonicalTarget: {
              database: "compilation_override_project",
              schema: "compilation_override_dataset",
              name: "dest",
            },
            targets: [
              {
                database: "compilation_override_project",
                schema: "compilation_override_dataset",
                name: "dest",
              },
              {
                database: "projectOverride",
                schema: "datasetOverride",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "compilation_override_project",
                schema: "compilation_override_dataset",
                name: "dest",
              },
              {
                database: "projectOverride",
                schema: "datasetOverride",
                name: "errorTable",
              },
            ],
            fileName: "definitions/data_preparation.sqlx",
            query: "FROM x\n|> SELECT *",
            load: {
              append: {},
            },
            errorTable: {
              database: "projectOverride",
              schema: "datasetOverride",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparations can be loaded via sqlx file with project defaults`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    name: "dest",
    errorTable: {
        name: "errorTable",
    },
    loadMode: {
        mode: "MAXIMUM",
        incrementalColumn: "xyz",
    },
}

FROM x
|> SELECT *
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.sqlx"),
        dataPreparationSqlx,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "dest",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "dest",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "dest",
              },
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "dest",
              },
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "errorTable",
              },
            ],
            fileName: "definitions/data_preparation.sqlx",
            load: {
              maximum: {
                columnName: "xyz",
              },
            },
            query: "FROM x\n|> SELECT *",
            errorTable: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparations can be loaded via an actions config file`, () => {
      const projectDir = createSimpleDataPreparationProject();
      const dataPreparationYaml = `
nodes:
- id: node1
  source:
    table:
      project: prj
      dataset: ds
      table: src
  destination:
    table:
      project: prj
      dataset: ds
      table: dest
  generated:
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
    sourceGenerated:
      sourceSchema:
        tableSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
    destinationGenerated:
      schema:
        field:
        - name: a
          type: STRING
          mode: NULLABLE
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.dp.yaml"),
        dataPreparationYaml,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "prj",
              schema: "ds",
              name: "dest",
            },
            canonicalTarget: {
              database: "prj",
              schema: "ds",
              name: "dest",
            },
            targets: [
              {
                database: "prj",
                schema: "ds",
                name: "dest",
              },
            ],
            canonicalTargets: [
              {
                database: "prj",
                schema: "ds",
                name: "dest",
              },
            ],
            fileName: "definitions/data_preparation.dp.yaml",
            dataPreparationYaml: dumpYaml(loadYaml(dataPreparationYaml)),
          },
        ]),
      );
    });

    test(`data preparations resolves compilation overrides before encoding`, () => {
      const projectDir = createSimpleDataPreparationProject(`
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
projectSuffix: projectSuffix
datasetSuffix: datasetSuffix
namePrefix: tablePrefix
`);
      const dataPreparationYaml = `
configuration:
  errorTable:
    table: error
nodes:
- id: node1
  source:
    table:
      table: src
  generated:
    sourceGenerated:
      sourceSchema:
        tableSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
- id: node2
  source:
    nodeId: node1
  destination:
    table:
      table: dest
  generated:
    sourceGenerated:
      sourceSchema:
        nodeSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
    destinationGenerated:
      schema:
        field:
        - name: a
          type: STRING
          mode: NULLABLE
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.dp.yaml"),
        dataPreparationYaml,
      );

      const resolvedYaml = `
configuration:
  errorTable:
    project: defaultProject_projectSuffix
    dataset: defaultDataset_datasetSuffix
    table: tablePrefix_error
nodes:
- id: node1
  source:
    table:
      project: defaultProject_projectSuffix
      dataset: defaultDataset_datasetSuffix
      table: tablePrefix_src
  generated:
    sourceGenerated:
      sourceSchema:
        tableSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
- id: node2
  source:
    nodeId: node1
  destination:
    table:
      project: defaultProject_projectSuffix
      dataset: defaultDataset_datasetSuffix
      table: tablePrefix_dest
  generated:
    sourceGenerated:
      sourceSchema:
        nodeSchema:
          field:
          - name: a
            type: STRING
            mode: NULLABLE
    outputSchema:
      field:
      - name: a
        type: INT64
        mode: NULLABLE
    destinationGenerated:
      schema:
        field:
        - name: a
          type: STRING
          mode: NULLABLE
`;

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject_projectSuffix",
              schema: "defaultDataset_datasetSuffix",
              name: "tablePrefix_dest",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "dest",
            },
            targets: [
              {
                database: "defaultProject_projectSuffix",
                schema: "defaultDataset_datasetSuffix",
                name: "tablePrefix_dest",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "dest",
              },
            ],
            fileName: "definitions/data_preparation.dp.yaml",
            dataPreparationYaml: dumpYaml(loadYaml(resolvedYaml)),
          },
        ]),
      );
    });

    test(`data preparation supports merge incremental config`, () => {
      const projectDir = createSimpleDataPreparationProject(VALID_WORKFLOW_SETTINGS_YAML, false);
      const dataPreparationSqlx = `
config {
    type: "dataPreparation",
    name: "dest",
    errorTable: {
        name: "errorTable",
    },
    loadMode: {
        mode: "MERGE",
        uniqueKey: ["a", "b"],
    },
}

FROM x
|> SELECT *
`;

      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.sqlx"),
        dataPreparationSqlx,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "dest",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "dest",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "dest",
              },
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "errorTable",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "dest",
              },
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "errorTable",
              },
            ],
            fileName: "definitions/data_preparation.sqlx",
            load: {
              merge: {
                uniqueKey: ["a", "b"],
              },
            },
            query: "FROM x\n|> SELECT *",
            errorTable: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "errorTable",
            },
            errorTableRetentionDays: 0,
          },
        ]),
      );
    });

    test(`data preparation supports defaultJobLabels and jobLabels in actions.yaml and sqlx`, () => {
      const workflowSettingsYaml = `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultJobLabels:
  env: dev
  team: dataprep_team
`;
      const projectDir = createSimpleDataPreparationProject(workflowSettingsYaml, false);
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- dataPreparation:
    filename: data_preparation.dp.yaml
    jobLabels:
      env: prod
      cost_center: finance`,
      );
      fs.writeFileSync(path.join(projectDir, "definitions/data_preparation.dp.yaml"), "\n");
      fs.writeFileSync(
        path.join(projectDir, "definitions/sqlx_dataprep.sqlx"),
        `
config {
  type: "dataPreparation",
  name: "dest",
  jobLabels: {
    env: "staging",
  },
}
FROM x
|> SELECT *
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const dpByFile = new Map(
        result.compile.compiledGraph.dataPreparations.map((dp) => [dp.fileName, dp]),
      );
      expect(
        asPlainObject(dpByFile.get("definitions/data_preparation.dp.yaml")?.actionDescriptor),
      ).deep.equals({
        jobLabels: {
          env: "prod",
          team: "dataprep_team",
          cost_center: "finance",
        },
      });
      expect(
        asPlainObject(dpByFile.get("definitions/sqlx_dataprep.sqlx")?.actionDescriptor),
      ).deep.equals({
        jobLabels: {
          env: "staging",
          team: "dataprep_team",
        },
      });
    });

    // actions.yaml rejects .sqlx filenames (actionConfigFilenameIsInvalidSqlx), so test fromObject directly.
    test(`data preparation supports numeric LoadMode enum values from DataPreparationConfig.fromObject`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const appendSqlxPath = path.join(projectDir, "append_dp.sqlx");
      const mergeSqlxPath = path.join(projectDir, "merge_dp.sqlx");
      fs.writeFileSync(appendSqlxPath, 'module.exports = { query: "FROM x\\n|> SELECT *" };');
      fs.writeFileSync(mergeSqlxPath, 'module.exports = { query: "FROM x\\n|> SELECT *" };');

      const session = new Session(
        projectDir,
        dataform.ProjectConfig.create({
          defaultDatabase: "defaultProject",
          defaultSchema: "defaultDataset",
        }),
      );
      const appendDp = new DataPreparation(
        session,
        dataform.ActionConfig.DataPreparationConfig.fromObject({
          name: "append_dest",
          filename: appendSqlxPath,
          loadMode: {
            mode: "APPEND",
          },
        }),
      );
      const mergeDp = new DataPreparation(
        session,
        dataform.ActionConfig.DataPreparationConfig.fromObject({
          name: "merge_dest",
          filename: mergeSqlxPath,
          loadMode: {
            mode: "MERGE",
            uniqueKey: ["id"],
          },
        }),
      );

      expect(asPlainObject([appendDp.compile(), mergeDp.compile()])).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "append_dest",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "append_dest",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "append_dest",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "append_dest",
              },
            ],
            fileName: appendSqlxPath,
            load: {
              append: {},
            },
            query: "FROM x\n|> SELECT *",
          },
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "merge_dest",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "merge_dest",
            },
            targets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "merge_dest",
              },
            ],
            canonicalTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "merge_dest",
              },
            ],
            fileName: mergeSqlxPath,
            load: {
              merge: {
                uniqueKey: ["id"],
              },
            },
            query: "FROM x\n|> SELECT *",
          },
        ]),
      );
    });

    test("fluent .database() and .schema() update target, canonicalTarget, targets and canonicalTargets with suffixes and prefixes", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const dpSqlxPath = path.join(projectDir, "fluent_dp.sqlx");
      fs.writeFileSync(dpSqlxPath, 'module.exports = { query: "FROM x\\n|> SELECT *" };');

      const session = new Session(
        projectDir,
        dataform.ProjectConfig.create({
          defaultDatabase: "defaultProject",
          defaultSchema: "defaultDataset",
          databaseSuffix: "ps",
          schemaSuffix: "ds",
          tablePrefix: "tp",
        }),
      );
      const dp = new DataPreparation(
        session,
        dataform.ActionConfig.DataPreparationConfig.fromObject({
          name: "fluent_dest",
          filename: dpSqlxPath,
          errorTable: {
            name: "err_tbl",
            project: "errProject",
            dataset: "errDataset",
          },
        }),
      )
        .database("customProject")
        .schema("customDataset");
      session.actions.push(dp);

      const compiledGraph = session.compile();

      expect(compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "customProject_ps",
              schema: "customDataset_ds",
              name: "tp_fluent_dest",
            },
            canonicalTarget: {
              database: "customProject",
              schema: "customDataset",
              name: "fluent_dest",
            },
            targets: [
              {
                database: "customProject_ps",
                schema: "customDataset_ds",
                name: "tp_fluent_dest",
              },
              {
                database: "errProject_ps",
                schema: "errDataset_ds",
                name: "tp_err_tbl",
              },
            ],
            canonicalTargets: [
              {
                database: "customProject",
                schema: "customDataset",
                name: "fluent_dest",
              },
              {
                database: "errProject",
                schema: "errDataset",
                name: "err_tbl",
              },
            ],
            errorTable: {
              database: "errProject_ps",
              schema: "errDataset_ds",
              name: "tp_err_tbl",
            },
            errorTableRetentionDays: 0,
            fileName: dpSqlxPath,
            load: {
              replace: {},
            },
            query: "FROM x\n|> SELECT *",
          },
        ]),
      );
    });

    test("fluent .database() and .schema() on YAML data preparation update targets and re-encode dataPreparationYaml", () => {
      const projectDir = createSimpleDataPreparationProject(`
defaultProject: defaultProject
defaultDataset: defaultDataset
projectSuffix: ps
datasetSuffix: ds
namePrefix: tp
`);
      fs.writeFileSync(
        path.join(projectDir, "definitions/data_preparation.dp.yaml"),
        `
nodes:
- id: node1
  destination:
    table:
      table: yaml_dest
`,
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/override.js"),
        `
dataform.actions[0].database("customProject").schema("customDataset");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "customProject_ps",
              schema: "customDataset_ds",
              name: "tp_yaml_dest",
            },
            canonicalTarget: {
              database: "customProject",
              schema: "customDataset",
              name: "yaml_dest",
            },
            targets: [
              {
                database: "customProject_ps",
                schema: "customDataset_ds",
                name: "tp_yaml_dest",
              },
            ],
            canonicalTargets: [
              {
                database: "customProject",
                schema: "customDataset",
                name: "yaml_dest",
              },
            ],
            fileName: "definitions/data_preparation.dp.yaml",
            dataPreparationYaml: dumpYaml({
              nodes: [
                {
                  id: "node1",
                  destination: {
                    table: {
                      project: "customProject_ps",
                      dataset: "customDataset_ds",
                      table: "tp_yaml_dest",
                    },
                  },
                },
              ],
            }),
          },
        ]),
      );
    });

    test("sqlx data preparation with suffixes and prefixes finalizes target once", () => {
      const projectDir = createSimpleDataPreparationProject(
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
projectSuffix: ps
datasetSuffix: ds
namePrefix: tp
`,
        false,
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/dp_suffix.sqlx"),
        `
config {
  type: "dataPreparation",
  name: "dest",
  project: "prj",
  dataset: "dsName",
}
FROM x
|> SELECT *
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.dataPreparations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "prj_ps",
              schema: "dsName_ds",
              name: "tp_dest",
            },
            canonicalTarget: {
              database: "prj",
              schema: "dsName",
              name: "dest",
            },
            targets: [
              {
                database: "prj_ps",
                schema: "dsName_ds",
                name: "tp_dest",
              },
            ],
            canonicalTargets: [
              {
                database: "prj",
                schema: "dsName",
                name: "dest",
              },
            ],
            fileName: "definitions/dp_suffix.sqlx",
            load: {
              replace: {},
            },
            query: "FROM x\n|> SELECT *",
          },
        ]),
      );
    });
  });
});
