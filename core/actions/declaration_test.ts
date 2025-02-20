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

suite("declaration", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test(`declarations can be loaded`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- declaration:
    name: action`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
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
          }
        }
      ])
    );
  });

  test(`fails when filename is defined for declaration`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- declaration:
    fileName: doesnotexist.sql
    name: name`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Unexpected property "fileName", or property value type of "string" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
    );
  });

  test(`fails when target name is not defined for declaration`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- declaration:
    dataset: test`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Declarations must have a populated 'name' field."
    );
  });

  suite("sqlx and JS API config options", () => {
    const declarationConfig = `{
    type: "declaration",
    name: "name",
    schema: "dataset",
    database: "project",
    description: "description",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
}`;
    [
      {
        filename: "declaration.sqlx",
        fileContents: `
config ${declarationConfig}`
      },
      {
        filename: "declaration.js",
        fileContents: `declare(${declarationConfig})`
      }
    ].forEach(testParameters => {
      test(`for declarations configured in a ${testParameters.filename} file`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, `definitions/${testParameters.filename}`),
          testParameters.fileContents
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
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
              fileName: `definitions/${testParameters.filename}`,
              actionDescriptor: exampleActionDescriptor.outputActionDescriptor
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
    fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- declaration:
    name: name
    dataset: dataset
    project: project
    description: description
`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
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
          actionDescriptor: {
            description: "description"
          }
        }
      ])
    );
  });
});
