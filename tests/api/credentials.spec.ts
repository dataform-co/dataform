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
      /Error reading credentials file: Unexpected end of JSON input/,
    );
  });

  test("bigquery empty credentials file", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(credentialsPath, "{}");
    expect(() => credentials.read(credentialsPath)).to.throw(
      /Error reading credentials file: the projectId field is required/,
    );
  });

  test("bigquery credentials file without key (ADC)", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({
        projectId: "my-gcp-project",
        location: "US",
      }),
    );
    const parsed = credentials.read(credentialsPath);
    expect(parsed.projectId).to.equal("my-gcp-project");
    expect(parsed.location).to.equal("US");
    expect(parsed.credentials).to.equal("");
  });

  test("bigquery credentials file with service account key", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    const keyJson = JSON.stringify({
      type: "service_account",
      client_email: "sa@my-gcp-project.iam.gserviceaccount.com",
      private_key: "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
    });
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({
        projectId: "my-gcp-project",
        location: "EU",
        credentials: keyJson,
      }),
    );
    const parsed = credentials.read(credentialsPath);
    expect(parsed.projectId).to.equal("my-gcp-project");
    expect(parsed.location).to.equal("EU");
    expect(parsed.credentials).to.equal(keyJson);
  });
});
