// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { exampleActionDescriptor } from "df/core/actions/index_test";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("operation", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`operations can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- operation:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
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
            queries: ["SELECT 1"],
            hermeticity: "NON_HERMETIC"
          }
        ])
      );
    });
  });

  suite("sqlx and JS API config options", () => {
    const operationConfig = `{
    type: "operations",
    name: "name",
    schema: "dataset",
    database: "project",
    dependencies: ["table"],
    tags: ["tagA", "tagB"],
    disabled: true,
    description: "description",
    hermetic: true,
    hasOutput: true,
    dependOnDependencyAssertions: true,
    bigqueryReservation: "reservation",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
}`;

    [
      {
        filename: "operation.sqlx",
        fileContents: `
config ${operationConfig}
SELECT 1`
      },
      {
        filename: "operation.js",
        fileContents: `operate("name", ${operationConfig}).queries(ctx => \`\n\nSELECT 1\`)`
      }
    ].forEach(testParameters => {
      test(`for operations configured in a ${testParameters.filename} file`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/table.sqlx"),
          `config {type: "view"} SELECT 1`
        );
        fs.writeFileSync(
          path.join(projectDir, `definitions/${testParameters.filename}`),
          testParameters.fileContents
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
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
              dependencyTargets: [
                {
                  database: "defaultProject",
                  schema: "defaultDataset",
                  name: "table"
                }
              ],
              disabled: true,
              fileName: `definitions/${testParameters.filename}`,
              hermeticity: "HERMETIC",
              hasOutput: true,
              tags: ["tagA", "tagB"],
              queries: ["\n\nSELECT 1"],
              actionDescriptor: {
                ...exampleActionDescriptor.outputActionDescriptor,
                bigqueryReservation: "reservation"
              }
            }
          ])
        );
      });
    });
  });

  test(`action config options`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/table.sqlx"),
      `config {type: "view"} SELECT 1`
    );
    fs.writeFileSync(path.join(projectDir, "definitions/filename.sql"), "SELECT 1");
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- operation:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
    - name: table
    filename: filename.sql
    tags:
    - tagA
    - tagB
    disabled: true
    hasOutput: true
    description: description
    dependOnDependencyAssertions: true
    hermetic: true
    bigqueryReservation: reservation
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
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
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "table"
            }
          ],
          disabled: true,
          fileName: "definitions/filename.sql",
          hermeticity: "HERMETIC",
          hasOutput: true,
          tags: ["tagA", "tagB"],
          queries: ["SELECT 1"],
          actionDescriptor: {
            description: "description",
            bigqueryReservation: "reservation"
          }
        }
      ])
    );
  });

  suite("jit compilation", () => {
    test("jit compilation is supported", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/op.js"),
        `operate("op").jitCode((ctx) => Promise.resolve("select 1"))`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "op"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "op"
          },
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/op.js",
          jitCode: '(ctx) => Promise.resolve("select 1")',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        }
      ]);
    });

    test("jit compilation fails if queries is also provided", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/op.js"),
        `operate("op").jitCode((ctx) => "select 1").queries("select 1")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors.some(e => e.message.includes("Cannot mix AoT and JiT compilation"))).equals(true);
    });
  });
});
