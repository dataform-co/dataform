// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  exampleActionDescriptor,
  exampleBuiltInAssertions,
  exampleBuiltInAssertionsAsYaml
} from "df/core/actions/index_test";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("view", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`views can be loaded`, () => {
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
- view:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

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
            type: "view",
            enumType: "VIEW",
            disabled: false
          }
        ])
      );
    });
  });

  suite("sqlx and JS API config options", () => {
    const viewConfig = `{
    type: "view",
    name: "name",
    schema: "dataset",
    database: "project",
    dependencies: ["operation"],
    tags: ["tag1", "tag2"],
    disabled: true,
    materialized: true,
    description: "description",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
    bigquery: {
    partitionBy: "partitionBy",
    clusterBy: ["clusterBy"],
    labels: {"key": "val"},
    additionalOptions: {
        option1Key: "option1",
        option2Key: "option2",
    }
    },
    dependOnDependencyAssertions: true,
    hermetic: true,
    ${exampleBuiltInAssertions.inputAssertionBlock}
    metadata: {
        overview: "view overview",
        extraProperties: {
            fields: {
                priority: { stringValue: "high" }
            }
        }
    },
}`;
    [
      {
        filename: "view.sqlx",
        fileContents: `
config ${viewConfig}
SELECT 1`
      },
      {
        filename: "view.js",
        fileContents: `publish("name", ${viewConfig}).query(ctx => \`\n\nSELECT 1\`)`
      }
    ].forEach(testParameters => {
      test(`for views configured in a ${testParameters.filename} file`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
        fs.writeFileSync(
          path.join(projectDir, `definitions/${testParameters.filename}`),
          testParameters.fileContents
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
            type: "view",
            disabled: true,
            hermeticity: "HERMETIC",
            bigquery: {
              clusterBy: ["clusterBy"],
              partitionBy: "partitionBy",
              additionalOptions: {
                option1Key: "option1",
                option2Key: "option2"
              },
              labels: {
                key: "val"
              }
            },
            tags: ["tag1", "tag2"],
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation"
              }
            ],
            enumType: "VIEW",
            fileName: `definitions/${testParameters.filename}`,
            query: "\n\nSELECT 1",
            actionDescriptor: {
              ...exampleActionDescriptor.outputActionDescriptor,
              // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
              bigqueryLabels: {
                key: "val"
              },
              metadata: {
                overview: "view overview",
                extraProperties: {
                  fields: {
                    priority: { stringValue: "high" }
                  }
                }
              },
            },
            materialized: true
          }
        ]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          exampleBuiltInAssertions.outputAssertions(testParameters.filename)
        );
      });
    });
  });

  test("action config options", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "definitions/filename.sql"), "SELECT 1");
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- view:
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
    materialized: true
    partitionBy: partitionBy
    clusterBy:
    - clusterBy
    description: description
    labels:
      key: val
    additionalOptions:
      option1Key: option1
      option2Key: option2
    dependOnDependencyAssertions: true
${exampleBuiltInAssertionsAsYaml.inputActionConfigBlock}
    hermetic: true
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
        type: "view",
        disabled: true,
        hermeticity: "HERMETIC",
        bigquery: {
          clusterBy: ["clusterBy"],
          partitionBy: "partitionBy",
          additionalOptions: {
            option1Key: "option1",
            option2Key: "option2"
          },
          labels: {
            key: "val"
          }
        },
        tags: ["tag1", "tag2"],
        dependencyTargets: [
          {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "operation"
          }
        ],
        enumType: "VIEW",
        fileName: "definitions/filename.sql",
        query: "SELECT 1",
        actionDescriptor: {
          bigqueryLabels: {
            key: "val"
          },
          description: "description"
        },
        materialized: true
      }
    ]);
    expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
      exampleBuiltInAssertionsAsYaml.outputAssertions
    );
  });

  suite("jit compilation", () => {
    test("jit compilation is supported", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/view.js"),
        `function jitF(jctx) {
          return Promise.resolve("select 1");
        }
        publish("viewF", {type: "view"}).jitCode(jitF);
        publish("viewArrow", {type: "view"}).jitCode((jctx) => Promise.resolve("select 1"));
        publish("viewStr", {type: "view"}).jitCode('(jctx) => Promise.resolve("select 1")')
        `
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewF"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewF"
          },
          type: "view",
          enumType: "VIEW",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/view.js",
          jitCode: 'function jitF(jctx) {\n          return Promise.resolve(\"select 1\");\n        }',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        },
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewArrow"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewArrow"
          },
          type: "view",
          enumType: "VIEW",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/view.js",
          jitCode: '(jctx) => Promise.resolve(\"select 1\")',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        },
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewStr"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "viewStr"
          },
          type: "view",
          enumType: "VIEW",
          disabled: false,
          hermeticity: "NON_HERMETIC",
          fileName: "definitions/view.js",
          jitCode: '(jctx) => Promise.resolve(\"select 1\")',
          actionDescriptor: {
            compilationMode: "ACTION_COMPILATION_MODE_JIT"
          }
        }
      ]);
    });

    test("jit compilation fails if query is also provided", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/view.js"),
        `publish("view", {type: "view"}).jitCode((ctx) => Promise.resolve("select 1")).query("select 1")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
      expect(result.compile.compiledGraph.graphErrors.compilationErrors.some(e => e.message.includes("Cannot mix AoT and JiT compilation"))).equals(true);
    });
  });
});
