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
          database: "defaultProject",
          schema: "defaultDataset",
          name: "dataset_name_assertions_uniqueKey_0",
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
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
          database: "defaultProject",
          schema: "defaultDataset",
          name: "dataset_name_assertions_rowConditions",
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
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
        database: "defaultProject",
        schema: "defaultDataset",
        name: "dataset_name_assertions_uniqueKey_0",
      },
      canonicalTarget: {
        database: "defaultProject",
        schema: "defaultDataset",
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
        database: "defaultProject",
        schema: "defaultDataset",
        name: "dataset_name_assertions_rowConditions",
      },
      canonicalTarget: {
        database: "defaultProject",
        schema: "defaultDataset",
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
              database: "defaultProject",
              schema: "defaultDataset",
              name: `builtin_dataset_name_assertions_uniqueKey_0`,
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
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
              database: "defaultProject",
              schema: "defaultDataset",
              name: `builtin_dataset_name_assertions_rowConditions`,
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
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

    if (["table", "view", "incremental"].includes(tableType)) {
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
    }
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
        "name: G",
        "jobLabels:",
        '  "goog-reserved": v',
        "entities:",
        "  - name: Account",
        "    dataSourceString: proj.ds.Accounts",
        "    keys: [id]",
      ].join("\n"),
    },
  ].forEach(({ name, file, content, extraFiles }) => {
    test(`${name} records compilation error when jobLabels has invalid key`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML,
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      if (extraFiles) {
        for (const [extraPath, extraContent] of Object.entries(extraFiles)) {
          fs.writeFileSync(path.join(projectDir, extraPath), extraContent);
        }
      }
      fs.writeFileSync(path.join(projectDir, file), content);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        'Invalid job label key "goog-reserved" in jobLabels: key cannot start with reserved prefix "goog-".',
      );
    });
  });
});
