import { config, expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { credentials } from "df/cli/api";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

config.truncateThreshold = 0;

suite("@dataform/api/credentials", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("bigquery blank credentials file", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(credentialsPath, "");
    expect(() => credentials.read(credentialsPath)).to.throw(
      /Error reading credentials file: Unexpected end of JSON input/
    );
  });

  test("bigquery empty credentials file", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(credentialsPath, "{}");
    expect(() => credentials.read(credentialsPath)).to.throw(
      /Error reading credentials file: the projectId field is required/
    );
  });
});
