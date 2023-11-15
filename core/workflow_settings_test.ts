import { suite, test } from "df/testing";
import { dataform } from "df/protos/ts";
import * as path from "path";
import * as fs from "fs-extra";
import { getWorkflowSettings } from "df/core/workflow_settings";
import { TmpDirFixture } from "df/testing/fixtures";

const VALID_WORKFLOW_SETTINGS_YAML = `
warehouse: "bigquery"
defaultDatabase: "tada-analytics"
defaultSchema: "dataform_example"
assertionSchema: "dataform_assertions"
defaultLocation: "US"
`;

const projectDir = path.join(process.env.TEST_TMPDIR, "project");

suite("@dataform/core", ({ beforeEach, afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const projectDir = tmpDirFixture.createNewTmpDir();

  test("workflow_settings", () => {
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    // getWorkflowSettings;
  });
});
