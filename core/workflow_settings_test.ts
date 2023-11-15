import { suite, test } from "df/testing";
import { dataform } from "df/protos/ts";
import * as path from "path";
import * as fs from "fs-extra";
import { readWorkflowSettings } from "df/core/workflow_settings";
import { TmpDirFixture } from "df/testing/fixtures";
import { expect } from "chai";

const VALID_WORKFLOW_SETTINGS_YAML = `
warehouse: "bigquery"
defaultDatabase: "tada-analytics"
defaultSchema: "dataform_example"
assertionSchema: "dataform_assertions"
defaultLocation: "US"
`;

// TODO: Add tests for:
// * Failed file reading.
// * Invalid JSON structure.
// * Invalid proto field content.
// * Fields not in the proto.

suite("@dataform/core", ({ beforeEach, afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const projectDir = tmpDirFixture.createNewTmpDir();

  test("workflow_settings", () => {
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
  });
});
