import { expect } from "chai";

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
} from "df/testing/run_core";

suite("assertion", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`assertions can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
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
    hermetic: true
    dependOnDependencyAssertions: true
    reservation: reservation
    jobLabels:
      job_key: job_val`,
      );
      writeDefinitionFile(projectDir, "action.sql", "SELECT 1");
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            actionDescriptor: {
              description: "description",
              reservation: "reservation",
              jobLabels: {
                job_key: "job_val",
              },
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
                database: "defaultProject",
              },
            ],
          },
        ]),
      );
    });

    test(`action config options`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, "filename.sql", "SELECT 1");
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
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
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: "name",
            },
            actionDescriptor: {
              description: "description",
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation",
              },
            ],
            disabled: true,
            fileName: "definitions/filename.sql",
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "SELECT 1",
          },
        ]),
      );
    });
  });

  suite("disableAssertions", () => {
    [
      {
        extension: "sqlx",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "assertion.sqlx",
            `config { type: "assertion" }SELECT 1 WHERE FALSE`,
          );
        },
      },
      {
        extension: "js",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "assertion.js",
            `assert("assertion").query("SELECT 1 WHERE FALSE");`,
          );
        },
      },
      {
        extension: "sql",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "actions.yaml",
            `
actions:
- assertion:
    name: assertion
    filename: assertion.sql`,
          );
          writeDefinitionFile(projectDir, "assertion.sql", "SELECT 1 WHERE FALSE");
        },
      },
    ].forEach((testCase) => {
      test(`disables ${testCase.extension} file assertions when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);

        testCase.setupFiles(projectDir);

        const coreRequest = coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            disableAssertions: true,
          }),
        );
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          asPlainObject([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "assertion",
                schema: "defaultDataset",
              },
              disabled: true,
              fileName: `definitions/assertion.${testCase.extension}`,
              query: "SELECT 1 WHERE FALSE",
              target: {
                database: "defaultProject",
                name: "assertion",
                schema: "defaultDataset",
              },
            },
          ]),
        );
      });
    });

    ["table", "view", "incremental"].forEach((tableType) => {
      test(`disables inline ${tableType} assertions when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "test.sqlx",
          `config {
            type: "${tableType}",
            assertions: {
              uniqueKey: ["id"],
              nonNull: ["name"],
              rowConditions: ["id > 0"]
            }
          }
          SELECT 1 as id, 'test' as name`,
        );

        const coreRequest = coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            disableAssertions: true,
          }),
        );
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          asPlainObject([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_uniqueKey_0",
                schema: "defaultDataset",
              },
              dependencyTargets: [
                {
                  database: "defaultProject",
                  name: "test",
                  schema: "defaultDataset",
                },
              ],
              disabled: true,
              fileName: "definitions/test.sqlx",
              parentAction: {
                database: "defaultProject",
                name: "test",
                schema: "defaultDataset",
              },
              query:
                "\nSELECT\n  *\nFROM (\n  SELECT\n    id,\n    COUNT(1) AS index_row_count\n  FROM `defaultProject.defaultDataset.test`\n  GROUP BY id\n  ) AS data\nWHERE index_row_count > 1\n",
              target: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_uniqueKey_0",
                schema: "defaultDataset",
              },
            },
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_rowConditions",
                schema: "defaultDataset",
              },
              dependencyTargets: [
                {
                  database: "defaultProject",
                  name: "test",
                  schema: "defaultDataset",
                },
              ],
              disabled: true,
              fileName: "definitions/test.sqlx",
              parentAction: {
                database: "defaultProject",
                name: "test",
                schema: "defaultDataset",
              },
              query:
                "\nSELECT\n  'id > 0' AS failing_row_condition,\n  *\nFROM `defaultProject.defaultDataset.test`\nWHERE NOT (id > 0\n)\nUNION ALL\nSELECT\n  'name IS NOT NULL' AS failing_row_condition,\n  *\nFROM `defaultProject.defaultDataset.test`\nWHERE NOT (name IS NOT NULL\n)\n",
              target: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_rowConditions",
                schema: "defaultDataset",
              },
            },
          ]),
        );
        expect(result.compile.compiledGraph.tables.length).equals(1);
      });
    });
  });

  suite("rowConditions SQL generation", () => {
    const compileRowConditionsQuery = (rowConditionsSource: string) => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "test.sqlx",
        `config {
  type: "table",
  assertions: {
    rowConditions: [${rowConditionsSource}]
  }
}
SELECT 1 AS a, 1 AS b`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      return result.compile.compiledGraph.assertions.find(
        (assertion) => assertion.target.name === "defaultDataset_test_assertions_rowConditions",
      ).query;
    };

    test("multiline conditions produce single-line failing_row_condition literals", () => {
      // Regression test for #2201: BigQuery rejects single-quoted literals that span lines.
      const query = compileRowConditionsQuery("`a > 0\n  AND b > 0`");

      expect(query).contains("'a > 0\\n  AND b > 0' AS failing_row_condition");
      // The WHERE clause keeps the condition exactly as authored.
      expect(query).contains("WHERE NOT (a > 0\n  AND b > 0\n)");
    });

    test("carriage returns, quotes and backslashes are escaped in the literal", () => {
      const condition = "CAST(a AS STRING) = r'x\\y'\r\nOR b > 0";
      const query = compileRowConditionsQuery(JSON.stringify(condition));

      expect(query).contains(
        String.raw`'CAST(a AS STRING) = r\'x\\y\'\r\nOR b > 0' AS failing_row_condition`,
      );
      expect(query).contains(`WHERE NOT (${condition}\n)`);
    });

    test("trailing line comments do not swallow the closing parenthesis", () => {
      const query = compileRowConditionsQuery(
        `"a > 0 -- must be positive", "b > 0 # hash comment"`,
      );

      expect(query).contains("WHERE NOT (a > 0 -- must be positive\n)");
      expect(query).contains("WHERE NOT (b > 0 # hash comment\n)");
    });

    test("actions.yaml block scalar conditions compile to valid SQL", () => {
      // Plain YAML "|" preserves the final newline, so even a one-line condition hit #2201.
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "test.sql", "SELECT 1 AS a");
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- table:
    filename: test.sql
    assertions:
      rowConditions:
        - |
          a > 0 -- must be positive
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const query = result.compile.compiledGraph.assertions.find(
        (assertion) => assertion.target.name === "defaultDataset_test_assertions_rowConditions",
      ).query;
      expect(query).contains("'a > 0 -- must be positive\\n' AS failing_row_condition");
      expect(query).contains("WHERE NOT (a > 0 -- must be positive\n\n)");
    });

    test("nonNull does not modify a rowConditions array shared between actions", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "tables.js",
        `
const sharedRowConditions = ["a > 0"];
publish("t1", { type: "table", assertions: { rowConditions: sharedRowConditions, nonNull: ["id"] } })
  .query("SELECT 1 AS a, 1 AS id");
publish("t2", { type: "table", assertions: { rowConditions: sharedRowConditions, nonNull: ["id"] } })
  .query("SELECT 1 AS a, 1 AS id");
publish("t3", { type: "table", assertions: { rowConditions: sharedRowConditions } })
  .query("SELECT 1 AS a");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const failingRowConditions = (tableName: string) =>
        result.compile.compiledGraph.assertions
          .find(
            (assertion) =>
              assertion.target.name === `defaultDataset_${tableName}_assertions_rowConditions`,
          )
          .query.match(/'[^']*' AS failing_row_condition/g);
      const expectedWithNonNull = [
        "'a > 0' AS failing_row_condition",
        "'id IS NOT NULL' AS failing_row_condition",
      ];
      expect(failingRowConditions("t1")).deep.equals(expectedWithNonNull);
      expect(failingRowConditions("t2")).deep.equals(expectedWithNonNull);
      expect(failingRowConditions("t3")).deep.equals(["'a > 0' AS failing_row_condition"]);
    });

    for (const nonNull of [[], ["id"]]) {
      test(`conditions appended after registration are retained with nonNull ${JSON.stringify(nonNull)}`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "table.js",
          `
const rowConditions = ["a > 0"];
publish("test", { type: "table", assertions: { rowConditions, nonNull: ${JSON.stringify(nonNull)} } })
  .query("SELECT 1 AS a, 1 AS b, 1 AS id");
rowConditions.push("b > 0");
`,
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        const query = result.compile.compiledGraph.assertions.find(
          (assertion) => assertion.target.name === "defaultDataset_test_assertions_rowConditions",
        ).query;
        expect(query.match(/'[^']*' AS failing_row_condition/g)).deep.equals([
          "'a > 0' AS failing_row_condition",
          "'b > 0' AS failing_row_condition",
          ...nonNull.map((column) => `'${column} IS NOT NULL' AS failing_row_condition`),
        ]);
        expect(query).contains("WHERE NOT (b > 0\n)");
      });
    }
  });

  test("JS assert().description() preserves jobLabels and reservation on actionDescriptor", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "assertion.js",
      `
assert("name", {
  reservation: "projects/p/locations/US/reservations/r",
  jobLabels: {
    env: "prod",
  },
})
  .description("my assertion description")
  .query("SELECT 1 WHERE FALSE");
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.assertions[0]?.actionDescriptor)).deep.equals(
      {
        description: "my assertion description",
        reservation: "projects/p/locations/US/reservations/r",
        jobLabels: {
          env: "prod",
        },
      },
    );
  });

  suite("Lakehouse (4-part) support and validation", () => {
    test("fails compilation when an assertion is configured with a lakehouse block", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "lh_assertion.sqlx",
        `
config {
  type: "assertion",
  lakehouse: { catalog: "c", namespace: "n" },
  name: "a"
}
SELECT 1
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
        "Lakehouse (4-part) targets are not supported for views and assertions",
      );
    });

    test("3-part standalone assertion querying a 4-part Lakehouse table via ref() compiles", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "lh_table.sqlx",
        `
config {
  type: "table",
  project: "p",
  lakehouse: { catalog: "c", namespace: "n" },
  name: "t"
}
SELECT 1 AS id
`,
      );
      writeDefinitionFile(
        projectDir,
        "check_lh.sqlx",
        `
config { type: "assertion" }
SELECT * FROM \${ref("p", "c", "n", "t")} WHERE id IS NULL
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const assertion = result.compile.compiledGraph.assertions[0];
      expect(asPlainObject(assertion.target)).deep.equals({
        database: "defaultProject",
        schema: "defaultDataset",
        name: "check_lh",
      });
      expect(assertion.query.trim()).equals("SELECT * FROM `p.c.n.t` WHERE id IS NULL");
      expect(asPlainObject(assertion.dependencyTargets)).deep.equals([
        {
          database: "p",
          schema: "c.n",
          name: "t",
        },
      ]);
    });
  });

  suite("jit compilation", () => {
    test("jit compilation is supported", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "assertion.js",
        `assert("assertion").jitCode((ctx) => Promise.resolve({query: "SELECT * FROM t WHERE invalid"}))`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "assertion",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "assertion",
          },
          fileName: "definitions/assertion.js",
          jitCode: '(ctx) => Promise.resolve({query: "SELECT * FROM t WHERE invalid"})',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT",
          },
        },
      ]);
    });

    test("jit compilation fails if query is also provided", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "assertion.js",
        `assert("assertion").jitCode((ctx) => Promise.resolve("SELECT 1")).query("SELECT 1")`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.some((e) =>
          e.message.includes("Assertion may set either .jitCode() or .query(), but not both."),
        ),
      ).equals(true);
    });

    test("jit compilation is supported in sqlx", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "assertion.sqlx",
        `config { type: "assertion", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1 WHERE FALSE`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "assertion",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "assertion",
          },
          fileName: "definitions/assertion.sqlx",
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
    const executionData = jctx.executionData ? jctx.executionData : undefined;
    
  const foo = "bar";

    return {
        query: (
          \`

SELECT 1 WHERE FALSE\`
        ),
      };
    }`,
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT",
          },
        },
      ]);
    });
  });
});
