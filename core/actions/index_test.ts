import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

export const exampleActionDescriptor = {
  inputSqlxConfigBlock: `
  columns: {
    column1Key: "column1Val",
    column2Key: {
      description: "description",
      columns: {
        nestedColumnKey: "nestedColumnVal"
      },
      tags: ["tag3", "tag4"],
      bigqueryPolicyTags: ["bigqueryPolicyTag1", "bigqueryPolicyTag2"],
    }
  },`,
  outputActionDescriptor: {
    columns: [
      {
        description: "column1Val",
        path: ["column1Key"],
      },
      {
        bigqueryPolicyTags: ["bigqueryPolicyTag1", "bigqueryPolicyTag2"],
        description: "description",
        path: ["column2Key"],
        tags: ["tag3", "tag4"],
      },
      {
        description: "nestedColumnVal",
        path: ["column2Key", "nestedColumnKey"],
      },
    ],
    description: "description",
  } as dataform.IColumnDescriptor,
};

export const exampleBuiltInAssertions = {
  inputAssertionBlock: `assertions: {
    uniqueKeys: [["uniqueKey1", "uniqueKey2"]],
    nonNull: "nonNull",
    rowConditions: ["rowConditions1", "rowConditions2"],
  },`,
  outputAssertions: (filename: string) =>
    [
      {
        actionDescriptor: {
          jobLabels: {
            job_key: "job_val",
          },
        },
        target: {
          database: "project",
          schema: "dataset",
          name: "dataset_name_assertions_uniqueKey_0",
        },
        canonicalTarget: {
          database: "project",
          schema: "dataset",
          name: "dataset_name_assertions_uniqueKey_0",
        },
        dependencyTargets: [
          {
            database: "project",
            schema: "dataset",
            name: "name",
          },
        ],
        disabled: true,
        fileName: `definitions/${filename}`,
        parentAction: {
          database: "project",
          schema: "dataset",
          name: "name",
        },
        query:
          "\nSELECT\n  *\nFROM (\n  SELECT\n    uniqueKey1, uniqueKey2,\n    COUNT(1) AS index_row_count\n  FROM `project.dataset.name`\n  GROUP BY uniqueKey1, uniqueKey2\n  ) AS data\nWHERE index_row_count > 1\n",
        tags: ["tag1", "tag2"],
      },
      {
        actionDescriptor: {
          jobLabels: {
            job_key: "job_val",
          },
        },
        target: {
          database: "project",
          schema: "dataset",
          name: "dataset_name_assertions_rowConditions",
        },
        canonicalTarget: {
          database: "project",
          schema: "dataset",
          name: "dataset_name_assertions_rowConditions",
        },
        dependencyTargets: [
          {
            database: "project",
            schema: "dataset",
            name: "name",
          },
        ],
        disabled: true,
        fileName: `definitions/${filename}`,
        parentAction: {
          database: "project",
          schema: "dataset",
          name: "name",
        },
        query:
          "\nSELECT\n  'rowConditions1' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions1)\nUNION ALL\nSELECT\n  'rowConditions2' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions2)\nUNION ALL\nSELECT\n  'nonNull IS NOT NULL' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (nonNull IS NOT NULL)\n",
        tags: ["tag1", "tag2"],
      },
    ] as dataform.IAssertion[],
};

export const exampleBuiltInAssertionsAsYaml = {
  inputActionConfigBlock: `
    assertions:
        uniqueKeys:
        - uniqueKey:
            - uniqueKey1
            - uniqueKey2
        nonNull:
        - nonNull
        rowConditions:
        - rowConditions1
        - rowConditions2
`,
  outputAssertions: [
    {
      actionDescriptor: {
        jobLabels: {
          job_key: "job_val",
        },
      },
      target: {
        database: "project",
        schema: "dataset",
        name: "dataset_name_assertions_uniqueKey_0",
      },
      canonicalTarget: {
        database: "project",
        schema: "dataset",
        name: "dataset_name_assertions_uniqueKey_0",
      },
      dependencyTargets: [
        {
          database: "project",
          schema: "dataset",
          name: "name",
        },
      ],
      disabled: true,
      // It would make more sense for this to be the path to the config, but we haven't yet
      // introduced a configFilename field.
      fileName: "definitions/filename.sql",
      parentAction: {
        database: "project",
        schema: "dataset",
        name: "name",
      },
      query:
        "\nSELECT\n  *\nFROM (\n  SELECT\n    uniqueKey1, uniqueKey2,\n    COUNT(1) AS index_row_count\n  FROM `project.dataset.name`\n  GROUP BY uniqueKey1, uniqueKey2\n  ) AS data\nWHERE index_row_count > 1\n",
      tags: ["tag1", "tag2"],
    },
    {
      actionDescriptor: {
        jobLabels: {
          job_key: "job_val",
        },
      },
      target: {
        database: "project",
        schema: "dataset",
        name: "dataset_name_assertions_rowConditions",
      },
      canonicalTarget: {
        database: "project",
        schema: "dataset",
        name: "dataset_name_assertions_rowConditions",
      },
      dependencyTargets: [
        {
          database: "project",
          schema: "dataset",
          name: "name",
        },
      ],
      disabled: true,
      // It would make more sense for this to be the path to the config, but we haven't yet
      // introduced a configFilename field.
      fileName: "definitions/filename.sql",
      parentAction: {
        database: "project",
        schema: "dataset",
        name: "name",
      },
      query:
        "\nSELECT\n  'rowConditions1' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions1)\nUNION ALL\nSELECT\n  'rowConditions2' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions2)\nUNION ALL\nSELECT\n  'nonNull IS NOT NULL' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (nonNull IS NOT NULL)\n",
      tags: ["tag1", "tag2"],
    },
  ] as dataform.IAssertion[],
};

suite("actions", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  const getActionsFromResult = (tableType: string, result: dataform.CoreExecutionResponse) => {
    switch (tableType) {
      case "table":
      case "view":
      case "incremental":
        return result.compile.compiledGraph.tables;
      case "operations":
        return result.compile.compiledGraph.operations;
      case "assertion":
        return result.compile.compiledGraph.assertions;
      default:
        throw Error(`Unexpected table type: ${tableType}`);
    }
  };

  ["table", "view", "incremental", "operations", "assertion"].forEach((tableType) => {
    test(`${tableType} target can be overridden by project config override`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        // If change, then change test "action configs assertions can be loaded".
        `
config {
  type: "${tableType}",
  name: "name",
}
SELECT 1`,
      );

      const result = runMainInVm(
        coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            defaultDatabase: "otherProject",
            defaultSchema: "otherDataset",
            assertionSchema: "otherDataset",
            tablePrefix: "prefix",
          }),
        ),
      );

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(getActionsFromResult(tableType, result)[0]?.target)).deep.equals({
        database: "otherProject",
        schema: "otherDataset",
        name: "prefix_name",
      });
      expect(
        asPlainObject(getActionsFromResult(tableType, result)[0]?.canonicalTarget),
      ).deep.equals({
        database: "otherProject",
        schema: "otherDataset",
        name: "name",
      });
    });
  });

  ["table", "view", "incremental"].forEach((tableType) => {
    test(`${tableType} target with builtin assertion and builtinAssertionNamePrefix set"`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
builtinAssertionNamePrefix: builtin
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  database: "project",
  schema: "dataset",
  name: "name",
  assertions: {
    uniqueKeys: [["key1", "key2"]],
    nonNull: "nonNull",
    rowConditions: ["condition"],
  }
}
  
SELECT 1`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "dataset",
              name: `builtin_dataset_name_assertions_uniqueKey_0`,
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: `builtin_dataset_name_assertions_uniqueKey_0`,
            },
            dependencyTargets: [
              {
                database: "project",
                schema: "dataset",
                name: "name",
              },
            ],
            fileName: `definitions/file.sqlx`,
            parentAction: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            query:
              "\nSELECT\n  *\nFROM (\n  SELECT\n    key1, key2,\n    COUNT(1) AS index_row_count\n  FROM `project.dataset.name`\n  GROUP BY key1, key2\n  ) AS data\nWHERE index_row_count > 1\n",
          },
          {
            target: {
              database: "project",
              schema: "dataset",
              name: `builtin_dataset_name_assertions_rowConditions`,
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: `builtin_dataset_name_assertions_rowConditions`,
            },
            dependencyTargets: [
              {
                database: "project",
                schema: "dataset",
                name: "name",
              },
            ],
            fileName: `definitions/file.sqlx`,
            parentAction: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            query:
              "\nSELECT\n  'condition' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (condition)\nUNION ALL\nSELECT\n  'nonNull IS NOT NULL' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (nonNull IS NOT NULL)\n",
          },
        ]),
      );
      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "defaultProject",
          defaultSchema: "defaultDataset",
          defaultLocation: "US",
          builtinAssertionNamePrefix: "builtin",
        }),
      );
    });

    test(`${tableType} built-in assertions use parent database and defaultAssertionDataset when available`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultAssertionDataset: assertionDataset
defaultLocation: US
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  database: "customProject",
  schema: "customDataset",
  name: "name",
  assertions: {
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) => asPlainObject(assertion.target)),
      ).deep.equals([
        {
          database: "customProject",
          schema: "assertionDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customProject",
          schema: "assertionDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) =>
          asPlainObject(assertion.canonicalTarget),
        ),
      ).deep.equals([
        {
          database: "customProject",
          schema: "assertionDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customProject",
          schema: "assertionDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ]);
    });

    test(`${tableType} built-in assertions fall back to defaultDataset when neither defaultAssertionDataset nor action schema is set`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  database: "customProject",
  name: "name",
  assertions: {
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) => asPlainObject(assertion.target)),
      ).deep.equals([
        {
          database: "customProject",
          schema: "defaultDataset",
          name: "defaultDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customProject",
          schema: "defaultDataset",
          name: "defaultDataset_name_assertions_rowConditions",
        },
      ]);
    });

    test(`${tableType} built-in assertions in JS API inherit database and schema from chained methods`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", { type: "${tableType}" })
  .assertions({
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  })
  .database("customProject")
  .schema("customDataset")
  .query("SELECT 1 AS id");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) => asPlainObject(assertion.target)),
      ).deep.equals([
        {
          database: "customProject",
          schema: "customDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customProject",
          schema: "customDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ]);
    });

    test(`${tableType} built-in assertions renamed by chained .schema() are resolved by new name in dependencies and targets`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", { type: "${tableType}" })
  .assertions({
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  })
  .schema("customDataset")
  .query("SELECT 1 AS id");

operate("downstream")
  .dependencies([
    "customDataset_name_assertions_uniqueKey_0",
    "customDataset_name_assertions_rowConditions",
  ])
  .queries("SELECT 1");

publish("downstream_with_assertions", {
  type: "view",
  dependOnDependencyAssertions: true,
}).query(ctx => \`SELECT * FROM \${ctx.ref("customDataset", "name")}\`);
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const expectedAssertionTargets = [
        {
          database: "defaultProject",
          schema: "customDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "defaultProject",
          schema: "customDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ];
      expect(
        asPlainObject(result.compile.compiledGraph.operations[0].dependencyTargets),
      ).deep.equals(expectedAssertionTargets);
      const downstreamView = result.compile.compiledGraph.tables.find(
        (t) => t.target.name === "downstream_with_assertions",
      );
      expect(asPlainObject(downstreamView.dependencyTargets)).deep.equals([
        {
          database: "defaultProject",
          schema: "customDataset",
          name: "name",
        },
        ...expectedAssertionTargets,
      ]);
      const compiledTargetNames = result.compile.compiledGraph.targets.map((t) => t.name);
      expect(compiledTargetNames).to.include("customDataset_name_assertions_uniqueKey_0");
      expect(compiledTargetNames).to.include("customDataset_name_assertions_rowConditions");
      expect(compiledTargetNames).to.not.include("defaultDataset_name_assertions_uniqueKey_0");
      expect(compiledTargetNames).to.not.include("defaultDataset_name_assertions_rowConditions");
    });

    test(`${tableType} built-in assertions renamed by chained .schema() cannot be resolved by old name`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", { type: "${tableType}" })
  .assertions({
    rowConditions: ["id > 0"],
  })
  .schema("customDataset")
  .query("SELECT 1 AS id");

operate("downstream")
  .dependencies(["defaultDataset_name_assertions_rowConditions"])
  .queries("SELECT 1");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "Missing dependency detected",
      );
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "defaultDataset_name_assertions_rowConditions",
      );
    });

    test(`${tableType} built-in assertions renamed by chained .schema() trigger duplicate-name check for the new name`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", { type: "${tableType}" })
  .assertions({
    rowConditions: ["id > 0"],
  })
  .schema("customDataset")
  .query("SELECT 1 AS id");

assert("customDataset_name_assertions_rowConditions", {
  dataset: "customDataset",
}).query("SELECT 1");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.assertions).deep.equals([]);
      const errorMessages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
        (e) => e.message,
      );
      expect(errorMessages.length).equals(2);
      errorMessages.forEach((message) => {
        expect(message).contains("Duplicate action name detected");
        expect(message).contains("customDataset_name_assertions_rowConditions");
      });
    });

    [
      { projectKey: "database", datasetKey: "schema" },
      { projectKey: "project", datasetKey: "dataset" },
    ].forEach(({ projectKey, datasetKey }) => {
      test(`${tableType} built-in assertions use assertions.${projectKey} and assertions.${datasetKey} over action-level and default values`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultAssertionDataset: assertionDataset
defaultLocation: US
`,
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/file.sqlx"),
          `
config {
  type: "${tableType}",
  database: "customProject",
  schema: "customDataset",
  name: "name",
  assertions: {
    ${projectKey}: "customAssertionProject",
    ${datasetKey}: "customAssertionDataset",
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        const expectedTargets = [
          {
            database: "customAssertionProject",
            schema: "customAssertionDataset",
            name: "customDataset_name_assertions_uniqueKey_0",
          },
          {
            database: "customAssertionProject",
            schema: "customAssertionDataset",
            name: "customDataset_name_assertions_rowConditions",
          },
        ];
        expect(
          result.compile.compiledGraph.assertions.map((assertion) =>
            asPlainObject(assertion.target),
          ),
        ).deep.equals(expectedTargets);
        expect(
          result.compile.compiledGraph.assertions.map((assertion) =>
            asPlainObject(assertion.canonicalTarget),
          ),
        ).deep.equals(expectedTargets);
        // The parent action's own target is unaffected, and the assertions still query it.
        const parentTarget = {
          database: "customProject",
          schema: "customDataset",
          name: "name",
        };
        expect(asPlainObject(result.compile.compiledGraph.tables[0].target)).deep.equals(
          parentTarget,
        );
        result.compile.compiledGraph.assertions.forEach((assertion) => {
          expect(assertion.query).contains("`customProject.customDataset.name`");
          expect(asPlainObject(assertion.parentAction)).deep.equals(parentTarget);
        });
      });
    });

    test(`${tableType} built-in assertions can override only the project or only the dataset`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/project_only.sqlx"),
        `
config {
  type: "${tableType}",
  assertions: {
    database: "customAssertionProject",
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/dataset_only.sqlx"),
        `
config {
  type: "${tableType}",
  assertions: {
    schema: "customAssertionDataset",
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions
          .map((assertion) => asPlainObject(assertion.target))
          .sort((a, b) => a.name.localeCompare(b.name)),
      ).deep.equals([
        {
          database: "defaultProject",
          schema: "customAssertionDataset",
          name: "defaultDataset_dataset_only_assertions_rowConditions",
        },
        {
          database: "customAssertionProject",
          schema: "defaultDataset",
          name: "defaultDataset_project_only_assertions_rowConditions",
        },
      ]);
    });

    test(`${tableType} built-in assertions in JS API keep assertions.database and assertions.schema after chained methods`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", { type: "${tableType}" })
  .assertions({
    database: "customAssertionProject",
    schema: "customAssertionDataset",
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  })
  .database("customProject")
  .schema("customDataset")
  .query("SELECT 1 AS id");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) => asPlainObject(assertion.target)),
      ).deep.equals([
        {
          database: "customAssertionProject",
          schema: "customAssertionDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customAssertionProject",
          schema: "customAssertionDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ]);
    });

    test(`${tableType} built-in assertions use assertions.project and assertions.dataset from actions.yaml`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultAssertionDataset: assertionDataset
defaultLocation: US
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/filename.sql"), "SELECT 1 AS id");
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- ${tableType === "incremental" ? "incrementalTable" : tableType}:
    name: name
    project: customProject
    dataset: customDataset
    filename: filename.sql
    assertions:
      project: customAssertionProject
      dataset: customAssertionDataset
      uniqueKey:
      - id
      rowConditions:
      - id > 0
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        result.compile.compiledGraph.assertions.map((assertion) => asPlainObject(assertion.target)),
      ).deep.equals([
        {
          database: "customAssertionProject",
          schema: "customAssertionDataset",
          name: "customDataset_name_assertions_uniqueKey_0",
        },
        {
          database: "customAssertionProject",
          schema: "customAssertionDataset",
          name: "customDataset_name_assertions_rowConditions",
        },
      ]);
    });
  });

  ["table", "view", "incremental", "operations", "assertion"].forEach((tableType) => {
    test(`${tableType} inherits defaultJobLabels and overrides colliding keys with action jobLabels`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultJobLabels:
  env: dev
  team: core
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  name: "name",
  jobLabels: {
    env: "prod",
    action_type: "${tableType}",
  }
}
SELECT 1`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(
        asPlainObject(getActionsFromResult(tableType, result)[0]?.actionDescriptor?.jobLabels),
      ).deep.equals({
        env: "prod",
        team: "core",
        action_type: tableType,
      });
    });
  });

  ["table", "view", "incremental"].forEach((tableType) => {
    test(`${tableType} built-in assertions inherit parent action jobLabels and defaultJobLabels`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `${VALID_WORKFLOW_SETTINGS_YAML}
defaultJobLabels:
  env: dev
  team: core
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  name: "name",
  jobLabels: {
    env: "prod",
    parent_label: "${tableType}",
  },
  assertions: {
    uniqueKey: ["id"],
    nonNull: ["id"],
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.assertions.length).equals(2);
      result.compile.compiledGraph.assertions.forEach((assertion) => {
        expect(asPlainObject(assertion.actionDescriptor?.jobLabels)).deep.equals({
          env: "prod",
          team: "core",
          parent_label: tableType,
        });
      });
    });

    test(`${tableType} built-in assertions do not inherit invalid parent jobLabels`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `${VALID_WORKFLOW_SETTINGS_YAML}
defaultJobLabels:
  team: core
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `
config {
  type: "${tableType}",
  name: "name",
  jobLabels: {
    "goog-reserved": "value"
  },
  assertions: {
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  }
}
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
      expect(result.compile.compiledGraph.assertions.length).equals(2);
      result.compile.compiledGraph.assertions.forEach((assertion) => {
        expect(asPlainObject(assertion.actionDescriptor?.jobLabels)).deep.equals({
          team: "core",
        });
      });
    });

    test(`${tableType} built-in assertions in JS API inherit parent action jobLabels`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `${VALID_WORKFLOW_SETTINGS_YAML}
defaultJobLabels:
  env: dev
  team: core
`,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name", {
  type: "${tableType}",
  jobLabels: {
    env: "prod",
    parent_label: "${tableType}",
  },
})
  .assertions({
    uniqueKey: ["id"],
    rowConditions: ["id > 0"],
  })
  .query("SELECT 1 AS id");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.assertions.length).equals(2);
      result.compile.compiledGraph.assertions.forEach((assertion) => {
        expect(asPlainObject(assertion.actionDescriptor?.jobLabels)).deep.equals({
          env: "prod",
          team: "core",
          parent_label: tableType,
        });
      });
    });
  });

  [
    {
      name: "table",
      file: "definitions/file.sqlx",
      content: 'config { type: "table", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "view",
      file: "definitions/file.sqlx",
      content: 'config { type: "view", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "incremental",
      file: "definitions/file.sqlx",
      content: 'config { type: "incremental", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "operations",
      file: "definitions/file.sqlx",
      content: 'config { type: "operations", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "assertion",
      file: "definitions/file.sqlx",
      content: 'config { type: "assertion", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "test",
      file: "definitions/file.sqlx",
      extraFiles: { "definitions/base.sqlx": 'config { type: "table" }\nSELECT 1' },
      content:
        'config { type: "test", dataset: "base", jobLabels: { "goog-reserved": "v" } }\nSELECT 1',
    },
    {
      name: "dataPreparation",
      file: "definitions/file.sqlx",
      content:
        'config { type: "dataPreparation", jobLabels: { "goog-reserved": "v" } }\nFROM x\n|> SELECT *',
    },
    {
      name: "propertyGraph",
      file: "definitions/graph.yaml",
      content: [
        "version: 0.2.0.dev0/google",
        "job_labels:",
        '  "goog-reserved": v',
        "semantic_model:",
        "  - name: G",
        "    entities:",
        "      - name: Account",
        "        source: bigquery:proj.ds.Accounts",
        "        primary_key: [id]",
      ].join("\n"),
    },
  ].forEach(({ name, file, content, extraFiles = {} }) => {
    test(`${name} records compilation error when jobLabels has invalid key`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      for (const [extraPath, extraContent] of Object.entries(extraFiles)) {
        fs.writeFileSync(path.join(projectDir, extraPath), extraContent);
      }
      fs.writeFileSync(path.join(projectDir, file), content);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        'Invalid job label key "goog-reserved" in jobLabels: key cannot start with reserved prefix "goog-".',
      );
    });
  });

  test("domain-scoped GCP project IDs compile and resolve refs while malformed or dotted project IDs fail", () => {
    const compileWith = (defaultProject: string, defs: Record<string, string>) => {
      const dir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(dir, "workflow_settings.yaml"),
        `defaultProject: ${defaultProject}\ndefaultDataset: ds\ndefaultLocation: US\n`,
      );
      fs.mkdirSync(path.join(dir, "definitions"));
      for (const [name, body] of Object.entries(defs)) {
        fs.writeFileSync(path.join(dir, "definitions", name), body);
      }
      return runMainInVm(coreExecutionRequestFromPath(dir)).compile.compiledGraph;
    };

    const validGraph = compileWith("google.com:my-project", {
      "base.sqlx": `config { type: "table" }\nSELECT 1 AS id`,
      "view.sqlx": `config { type: "view", database: "sub.domain.com:other" }\nSELECT * FROM \${ref("base")}`,
    });
    expect(validGraph.graphErrors.compilationErrors).deep.equals([]);
    const baseTarget = { database: "google.com:my-project", schema: "ds", name: "base" };
    const viewTarget = { database: "sub.domain.com:other", schema: "ds", name: "view" };
    expect(asPlainObject(validGraph.tables)).deep.equals(
      asPlainObject([
        {
          target: baseTarget,
          canonicalTarget: baseTarget,
          type: "table",
          enumType: "TABLE",
          disabled: false,
          fileName: "definitions/base.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\nSELECT 1 AS id",
        },
        {
          target: viewTarget,
          canonicalTarget: viewTarget,
          dependencyTargets: [baseTarget],
          type: "view",
          enumType: "VIEW",
          disabled: false,
          fileName: "definitions/view.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\nSELECT * FROM `google.com:my-project.ds.base`",
        },
      ]),
    );

    const invalidGraph = compileWith("google.com:my-project.typo_ds", {
      "t1.sqlx": `config { type: "table" }\nSELECT 1`,
      "t2.sqlx": `config { type: "table", database: "my-project.typo_ds" }\nSELECT 1`,
      "t3.sqlx": `config { type: "table", database: ":proj" }\nSELECT 1`,
      "t4.sqlx": `config { type: "table", database: "dom:" }\nSELECT 1`,
      "t5.sqlx": `config { type: "table", database: "a:b:c" }\nSELECT 1`,
    });
    expect(
      invalidGraph.graphErrors.compilationErrors.map(({ message, fileName, actionTarget }) => ({
        message,
        fileName,
        actionTarget: asPlainObject(actionTarget),
      })),
      // t1 inherits the dotted default project: workflow settings defaults are not validated, only
      // projects set explicitly on an action are.
    ).deep.equals([
      {
        message: "Action target projects cannot include '.'",
        fileName: "definitions/t2.sqlx",
        actionTarget: { database: "my-project.typo_ds", schema: "ds", name: "t2" },
      },
      {
        message:
          "Action target projects with ':' must follow '<domain>:<project>' with non-empty segments",
        fileName: "definitions/t3.sqlx",
        actionTarget: { database: ":proj", schema: "ds", name: "t3" },
      },
      {
        message:
          "Action target projects with ':' must follow '<domain>:<project>' with non-empty segments",
        fileName: "definitions/t4.sqlx",
        actionTarget: { database: "dom:", schema: "ds", name: "t4" },
      },
      {
        message:
          "Action target projects with ':' must follow '<domain>:<project>' with non-empty segments",
        fileName: "definitions/t5.sqlx",
        actionTarget: { database: "a:b:c", schema: "ds", name: "t5" },
      },
    ]);
  });

  suite("conflicting legacy and modern config aliases", () => {
    const compileProject = (files: { [name: string]: string }) => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      for (const [name, content] of Object.entries(files)) {
        fs.writeFileSync(path.join(projectDir, "definitions", name), content);
      }
      return runMainInVm(coreExecutionRequestFromPath(projectDir)).compile.compiledGraph;
    };

    for (const actionType of [
      "table",
      "view",
      "incremental",
      "operations",
      "assertion",
      "dataPreparation",
    ]) {
      test(`reports compile errors for conflicting aliases in ${actionType} sqlx config`, () => {
        const graph = compileProject({
          "dep.sqlx": `config { type: "table" }\nSELECT 1`,
          "action.sqlx": `
config {
  type: "${actionType}",
  dataset: "new_dataset",
  schema: "old_schema",
  project: "new_project",
  database: "old_database",
  filename: "definitions/action.sqlx",
  fileName: "old_file.sqlx",
  dependencyTargets: [{ name: "dep" }],
  dependencies: ["dep"],
}
SELECT 1`,
        });

        expect(graph.graphErrors.compilationErrors.map((e) => e.message)).deep.equals([
          "'dependencies' is a legacy alias for 'dependencyTargets'; cannot specify both at the same time. It's recommended to use 'dependencyTargets'.",
          "'database' is a legacy alias for 'project'; cannot specify both at the same time. It's recommended to use 'project'.",
          "'schema' is a legacy alias for 'dataset'; cannot specify both at the same time. It's recommended to use 'dataset'.",
          "'fileName' is a legacy alias for 'filename'; cannot specify both at the same time. It's recommended to use 'filename'.",
        ]);
      });
    }

    test("reports compile errors for conflicting aliases in declaration sqlx config", () => {
      const graph = compileProject({
        "decl.sqlx": `
config {
  type: "declaration",
  name: "my_decl",
  dataset: "new_dataset",
  schema: "old_schema",
  project: "new_project",
  database: "old_database",
  filename: "definitions/action.sqlx",
  fileName: "old_file.sqlx",
}`,
      });

      expect(graph.graphErrors.compilationErrors.map((e) => e.message)).deep.equals([
        "'database' is a legacy alias for 'project'; cannot specify both at the same time. It's recommended to use 'project'.",
        "'schema' is a legacy alias for 'dataset'; cannot specify both at the same time. It's recommended to use 'dataset'.",
        "'fileName' is a legacy alias for 'filename'; cannot specify both at the same time. It's recommended to use 'filename'.",
      ]);
    });

    test("reports compile errors for conflicting aliases in inline assertions (sqlx and js api)", () => {
      const graph = compileProject({
        "t.sqlx": `
config {
  type: "table",
  assertions: {
    uniqueKey: ["a"],
    dataset: "new_ds",
    schema: "old_schema",
    project: "new_proj",
    database: "old_db",
  }
}
SELECT 1 AS a`,
        "js_table.js": `
publish("js_t", { type: "table" })
  .assertions({
    uniqueKey: ["a"],
    dataset: "new_ds",
    schema: "old_schema",
    project: "new_proj",
    database: "old_db",
  })
  .query("SELECT 1 AS a");`,
      });

      expect(
        graph.graphErrors.compilationErrors.map((e) => ({
          message: e.message,
          fileName: e.fileName,
        })),
      ).deep.equals([
        {
          message:
            "'assertions.database' is a legacy alias for 'assertions.project'; cannot specify both at the same time. It's recommended to use 'assertions.project'.",
          fileName: "definitions/js_table.js",
        },
        {
          message:
            "'assertions.schema' is a legacy alias for 'assertions.dataset'; cannot specify both at the same time. It's recommended to use 'assertions.dataset'.",
          fileName: "definitions/js_table.js",
        },
        {
          message:
            "'assertions.database' is a legacy alias for 'assertions.project'; cannot specify both at the same time. It's recommended to use 'assertions.project'.",
          fileName: "definitions/t.sqlx",
        },
        {
          message:
            "'assertions.schema' is a legacy alias for 'assertions.dataset'; cannot specify both at the same time. It's recommended to use 'assertions.dataset'.",
          fileName: "definitions/t.sqlx",
        },
      ]);
    });

    test("reports compile errors for conflicting aliases in DataPreparation config() helper", () => {
      const graph = compileProject({
        "dp.dp.sqlx": `
config {
  type: "dataPreparation",
}
FROM x
|> SELECT *
\${ctx.config({
  dataset: "new_ds",
  schema: "old_schema",
  project: "new_proj",
  database: "old_db",
})}`,
      });

      expect(
        graph.graphErrors.compilationErrors.map((e) => ({
          message: e.message,
          fileName: e.fileName,
        })),
      ).deep.equals([
        {
          message:
            "'database' is a legacy alias for 'project'; cannot specify both at the same time. It's recommended to use 'project'.",
          fileName: "definitions/dp.dp.sqlx",
        },
        {
          message:
            "'schema' is a legacy alias for 'dataset'; cannot specify both at the same time. It's recommended to use 'dataset'.",
          fileName: "definitions/dp.dp.sqlx",
        },
      ]);
      expect(asPlainObject(graph.dataPreparations[0].target)).deep.equals({
        database: "new_proj",
        schema: "new_ds",
        name: "dp",
      });
    });

    for (const actionType of ["table", "view", "incremental", "operations", "assertion"]) {
      test(`accepts a single-value dependencies field in ${actionType} sqlx config`, () => {
        const graph = compileProject({
          "dep.sqlx": `config { type: "table" }\nSELECT 1`,
          "action.sqlx": `
config {
  type: "${actionType}",
  dependencies: "dep",
}
SELECT 1`,
        });

        expect(graph.graphErrors.compilationErrors).deep.equals([]);
        const action = [...graph.tables, ...graph.operations, ...graph.assertions].find(
          (a) => a.target.name === "action",
        );
        expect(asPlainObject(action.dependencyTargets)).deep.equals([
          {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "dep",
          },
        ]);
      });
    }

    test("reports an invalid legacy dependency as a compile error without aborting the file", () => {
      const graph = compileProject({
        "actions.js": `
publish("first", { type: "table", dependencies: ["a.b.c.d"] }).query("SELECT 1");
publish("second", { type: "table" }).query("SELECT 1");`,
      });

      expect(
        graph.graphErrors.compilationErrors.map((e) => ({
          message: e.message,
          fileName: e.fileName,
        })),
      ).deep.equals([
        {
          message:
            "Invalid dependency. Accepted inputs include: a string of the form 'name', " +
            "'dataset.name' or 'project.dataset.name', or a target object.",
          fileName: "definitions/actions.js",
        },
      ]);
      expect(graph.tables.map((t) => t.target.name).sort()).deep.equals(["first", "second"]);
    });

    const aliasConflictErrors = (fileName: string) =>
      [
        ["database", "project"],
        ["schema", "dataset"],
      ].map(([legacyKey, modernKey]) => ({
        message:
          `'${legacyKey}' is a legacy alias for '${modernKey}'; cannot specify both at the ` +
          `same time. It's recommended to use '${modernKey}'.`,
        fileName,
      }));
    const compilationErrors = (graph: dataform.ICompiledGraph) =>
      graph.graphErrors.compilationErrors.map((e) => ({
        message: e.message,
        fileName: e.fileName,
      }));

    for (const { description, legacyValue } of [
      { description: "both names", legacyValue: `"legacy"` },
      { description: "an empty legacy value alongside the modern name", legacyValue: `""` },
    ]) {
      test(`reports compile errors for ${description} in JS assert(), declare() and operate()`, () => {
        const graph = compileProject({
          "assert.js": `
assert("a", {
  dataset: "ds", schema: ${legacyValue}, project: "proj", database: ${legacyValue},
}).query("SELECT 1");`,
          "declare.js": `
declare({
  name: "d", dataset: "ds", schema: ${legacyValue}, project: "proj", database: ${legacyValue},
});`,
          "operate.js": `
operate("o", {
  dataset: "ds", schema: ${legacyValue}, project: "proj", database: ${legacyValue},
}).queries("SELECT 1");`,
        });

        expect(compilationErrors(graph)).deep.equals([
          ...aliasConflictErrors("definitions/assert.js"),
          ...aliasConflictErrors("definitions/declare.js"),
          ...aliasConflictErrors("definitions/operate.js"),
        ]);
      });
    }

    test("treats a null legacy value like a null modern value in JS assert(), declare() and operate()", () => {
      const graph = compileProject({
        "assert_legacy.js": `assert("a1", { database: null }).query("SELECT 1");`,
        "assert_modern.js": `assert("a2", { project: null }).query("SELECT 1");`,
        "declare_legacy.js": `declare({ name: "d1", dataset: "ds", database: null });`,
        "declare_modern.js": `declare({ name: "d2", dataset: "ds", project: null });`,
        "operate_legacy.js": `operate("o1", { database: null }).queries("SELECT 1");`,
        "operate_modern.js": `operate("o2", { project: null }).queries("SELECT 1");`,
      });

      const emptyProjectError = (configType: string, fileName: string) => ({
        message:
          `Unexpected empty value for "project". See https://dataform-co.github.io/dataform/` +
          `docs/reference/configs#dataform-ActionConfig-${configType} for allowed properties.`,
        fileName,
      });
      expect(compilationErrors(graph)).deep.equals([
        emptyProjectError("AssertionConfig", "definitions/assert_legacy.js"),
        emptyProjectError("AssertionConfig", "definitions/assert_modern.js"),
        emptyProjectError("DeclarationConfig", "definitions/declare_legacy.js"),
        emptyProjectError("DeclarationConfig", "definitions/declare_modern.js"),
        emptyProjectError("OperationConfig", "definitions/operate_legacy.js"),
        emptyProjectError("OperationConfig", "definitions/operate_modern.js"),
      ]);
    });

    test("treats an empty legacy value like an empty modern value in JS assert(), declare() and operate()", () => {
      const graph = compileProject({
        "assert_legacy.js": `assert("a1", { schema: "", database: "" }).query("SELECT 1");`,
        "assert_modern.js": `assert("a2", { dataset: "", project: "" }).query("SELECT 1");`,
        "declare_legacy.js": `declare({ name: "d1", schema: "", database: "" });`,
        "declare_modern.js": `declare({ name: "d2", dataset: "", project: "" });`,
        "operate_legacy.js": `operate("o1", { schema: "", database: "" }).queries("SELECT 1");`,
        "operate_modern.js": `operate("o2", { dataset: "", project: "" }).queries("SELECT 1");`,
      });

      expect(compilationErrors(graph)).deep.equals([]);
      const targetWithoutName = (action: { target?: dataform.ITarget }) => ({
        database: action.target.database,
        schema: action.target.schema,
      });
      const [a1, a2] = ["a1", "a2"].map((name) =>
        graph.assertions.find((a) => a.target.name === name),
      );
      const [d1, d2] = ["d1", "d2"].map((name) =>
        graph.declarations.find((d) => d.target.name === name),
      );
      const [o1, o2] = ["o1", "o2"].map((name) =>
        graph.operations.find((o) => o.target.name === name),
      );
      // An empty value behaves as unset under either name, so the defaults apply.
      const defaults = { database: "defaultProject", schema: "defaultDataset" };
      [a1, a2, d1, d2, o1, o2].forEach((action) =>
        expect(targetWithoutName(action)).deep.equals(defaults),
      );
    });

    test("accepts legacy names in JS operate() without a type", () => {
      const graph = compileProject({
        "dep.sqlx": `config { type: "table" }\nSELECT 1`,
        "js_op.js": `
operate("js_op", {
  schema: "s",
  database: "d",
  dependencies: ["dep"],
  hasOutput: true,
  columns: { col: "desc" },
}).queries("SELECT 1");`,
      });

      expect(compilationErrors(graph)).deep.equals([]);
      expect(asPlainObject(graph.operations[0])).deep.equals({
        target: {
          database: "d",
          schema: "s",
          name: "js_op",
        },
        canonicalTarget: {
          database: "d",
          schema: "s",
          name: "js_op",
        },
        dependencyTargets: [{ database: "defaultProject", schema: "defaultDataset", name: "dep" }],
        fileName: "definitions/js_op.js",
        hermeticity: "NON_HERMETIC",
        hasOutput: true,
        queries: ["SELECT 1"],
        actionDescriptor: {
          columns: [
            {
              description: "desc",
              path: ["col"],
            },
          ],
        },
      });
    });

    test("allows legacy fileName alone without conflicting with auto-populated filename", () => {
      const graph = compileProject({
        "sqlx_table.sqlx": `
config {
  type: "table",
  fileName: "custom/sqlx_table.sqlx",
}
SELECT 1`,
        "js_actions.js": `
publish("js_table", { type: "table", fileName: "custom/js_table.sql" }).query("SELECT 1");
assert("js_assert", { fileName: "custom/js_assert.sql" }).query("SELECT 1");
operate("js_op", { fileName: "custom/js_op.sql" }).queries("SELECT 1");`,
      });

      expect(graph.graphErrors.compilationErrors).deep.equals([]);
      expect(graph.tables.find((t) => t.target.name === "sqlx_table").fileName).equals(
        "custom/sqlx_table.sqlx",
      );
      expect(graph.tables.find((t) => t.target.name === "js_table").fileName).equals(
        "custom/js_table.sql",
      );
      expect(graph.assertions.find((a) => a.target.name === "js_assert").fileName).equals(
        "custom/js_assert.sql",
      );
      expect(graph.operations.find((o) => o.target.name === "js_op").fileName).equals(
        "custom/js_op.sql",
      );
    });

    test("allows legacy schema and database alone in DataPreparation sqlx config", () => {
      const graph = compileProject({
        "dp.sqlx": `
config {
  type: "dataPreparation",
  schema: "legacy_schema",
  database: "legacy_db",
}
SELECT 1`,
      });

      expect(graph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(graph.dataPreparations[0].target)).deep.equals({
        database: "legacy_db",
        schema: "legacy_schema",
        name: "dp",
      });
    });
  });
  suite("target validation with workflow settings defaults", () => {
    const compileWith = (workflowSettings: string, defs: Record<string, string>) => {
      const dir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(dir, "workflow_settings.yaml"), workflowSettings);
      fs.mkdirSync(path.join(dir, "definitions"));
      for (const [name, body] of Object.entries(defs)) {
        fs.writeFileSync(path.join(dir, "definitions", name), body);
      }
      return runMainInVm(coreExecutionRequestFromPath(dir)).compile.compiledGraph;
    };

    const defaultSettings = `defaultProject: my-project\ndefaultDataset: ds\ndefaultLocation: US\n`;
    const noDatasetSettings = `defaultProject: my-project\ndefaultLocation: US\n`;

    const allActionTypes = {
      "table.sqlx": `config { type: "table" }\nSELECT 1 AS id`,
      "view.sqlx": `config { type: "view" }\nSELECT 1 AS id`,
      "incremental.sqlx": `config { type: "incremental" }\nSELECT 1 AS id`,
      "operation.sqlx": `config { type: "operations", hasOutput: true }\nCREATE TABLE \${self()} AS SELECT 1 AS id`,
      "assertion.sqlx": `config { type: "assertion" }\nSELECT 1 AS id WHERE FALSE`,
    };

    [".leading_dot_ds", "dotted.ds"].forEach((defaultDataset) => {
      test(`defaultDataset "${defaultDataset}" containing '.' compiles without errors`, () => {
        const graph = compileWith(
          `defaultProject: my-project\ndefaultDataset: ${defaultDataset}\ndefaultLocation: US\n`,
          {
            ...allActionTypes,
            "view_with_project.sqlx": `config { type: "view", project: "other-project" }\nSELECT 1 AS id`,
            "with_assertions.sqlx": `config { type: "table", assertions: { uniqueKey: ["id"], nonNull: ["id"] } }\nSELECT 1 AS id`,
          },
        );
        expect(graph.graphErrors.compilationErrors).deep.equals([]);
        expect(graph.tables.length).equals(5);
        graph.tables.forEach((table) => expect(table.target.schema).equals(defaultDataset));
        expect(graph.operations[0].target.schema).equals(defaultDataset);
        // Inline assertion names embed the dataset of the parent action, with '.' replaced by '_'.
        const namePrefix = `${defaultDataset.replace(/\./g, "_")}_with_assertions_assertions`;
        expect(graph.assertions.map((assertion) => assertion.target.name)).to.have.members([
          "assertion",
          `${namePrefix}_uniqueKey_0`,
          `${namePrefix}_rowConditions`,
        ]);
        graph.assertions.forEach((assertion) =>
          expect(assertion.target.schema).equals(defaultDataset),
        );
      });
    });

    ["dotted.project", "google.com:my-project.typo_ds"].forEach((defaultProject) => {
      test(`defaultProject "${defaultProject}" containing '.' compiles without errors`, () => {
        const graph = compileWith(
          `defaultProject: "${defaultProject}"\ndefaultDataset: ds\ndefaultLocation: US\n`,
          {
            ...allActionTypes,
            "with_assertions.sqlx": `config { type: "table", assertions: { uniqueKey: ["id"], nonNull: ["id"] } }\nSELECT 1 AS id`,
          },
        );
        expect(graph.graphErrors.compilationErrors).deep.equals([]);
        [...graph.tables, ...graph.operations, ...graph.assertions].forEach((action) =>
          expect(action.target.database).equals(defaultProject),
        );
      });
    });

    Object.entries({
      table: `config { type: "table", CONFIG }\nSELECT 1 AS id`,
      view: `config { type: "view", CONFIG }\nSELECT 1 AS id`,
      incremental: `config { type: "incremental", CONFIG }\nSELECT 1 AS id`,
      operations: `config { type: "operations", hasOutput: true, CONFIG }\nSELECT 1 AS id`,
      assertion: `config { type: "assertion", CONFIG }\nSELECT 1 AS id WHERE FALSE`,
    }).forEach(([type, definition]) => {
      [
        { config: `name: "dotted.name"`, message: "Action target names cannot include '.'" },
        { config: `dataset: "dotted.ds"`, message: "Action target datasets cannot include '.'" },
        {
          config: `project: "dotted.project"`,
          message: "Action target projects cannot include '.'",
        },
      ].forEach(({ config, message }) => {
        test(`${type} with explicitly configured ${config} fails compilation exactly once`, () => {
          const graph = compileWith(defaultSettings, {
            "t.sqlx": definition.replace("CONFIG", config),
          });
          expect(
            graph.graphErrors.compilationErrors.map(({ message: errorMessage, fileName }) => ({
              message: errorMessage,
              fileName,
            })),
          ).deep.equals([{ message, fileName: "definitions/t.sqlx" }]);
        });
      });
    });

    [".assertions", "dotted.assertions"].forEach((defaultAssertionDataset) => {
      test(`defaultAssertionDataset "${defaultAssertionDataset}" containing '.' compiles for standalone and inline assertions`, () => {
        const graph = compileWith(
          `defaultProject: my-project\ndefaultDataset: ds\ndefaultAssertionDataset: ${defaultAssertionDataset}\ndefaultLocation: US\n`,
          {
            "table.sqlx": `config { type: "table", assertions: { uniqueKey: ["id"], nonNull: ["id"] } }\nSELECT 1 AS id`,
            "view.sqlx": `config { type: "view", assertions: { nonNull: ["id"] } }\nSELECT 1 AS id`,
            "incremental.sqlx": `config { type: "incremental", assertions: { nonNull: ["id"] } }\nSELECT 1 AS id`,
            "js_table.js": `publish("js_table", { type: "table", assertions: { nonNull: ["id"] } }).query("SELECT 1 AS id");`,
            "assertion.sqlx": `config { type: "assertion" }\nSELECT 1 AS id WHERE FALSE`,
          },
        );
        expect(graph.graphErrors.compilationErrors).deep.equals([]);
        expect(graph.assertions.length).equals(6);
        graph.assertions.forEach((assertion) => {
          expect(assertion.target.schema).equals(defaultAssertionDataset);
        });
      });
    });

    test("explicitly configured assertions.dataset containing '.' still fails compilation", () => {
      const graph = compileWith(defaultSettings, {
        "t.sqlx": `config { type: "table", assertions: { dataset: "dotted.assertions", nonNull: ["id"] } }\nSELECT 1 AS id`,
      });
      expect(
        graph.graphErrors.compilationErrors.map(({ message, fileName }) => ({
          message,
          fileName,
        })),
      ).to.include.deep.members([
        { message: "Action target datasets cannot include '.'", fileName: "definitions/t.sqlx" },
      ]);
    });

    test("actions compile without errors when no dataset is set anywhere", () => {
      const graph = compileWith(noDatasetSettings, allActionTypes);
      expect(graph.graphErrors.compilationErrors).deep.equals([]);
    });

    test("actions compile without errors when no project is set anywhere", () => {
      const graph = compileWith(`defaultDataset: ds\ndefaultLocation: US\n`, allActionTypes);
      expect(graph.graphErrors.compilationErrors).deep.equals([]);
    });
  });
});
