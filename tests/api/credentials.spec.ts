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
      private_key: "fake-private-key",
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

  test("bigquery credentials file with a service account to impersonate", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({
        projectId: "my-gcp-project",
        location: "US",
        impersonateServiceAccount: "sa@my-gcp-project.iam.gserviceaccount.com",
      }),
    );
    const parsed = credentials.read(credentialsPath);
    expect(parsed.projectId).to.equal("my-gcp-project");
    expect(parsed.impersonateServiceAccount).to.equal("sa@my-gcp-project.iam.gserviceaccount.com");
    expect(parsed.credentials).to.equal("");
  });

  test("bigquery credentials file with an unknown field", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const credentialsPath = path.join(projectDir, "credentials.json");
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({
        projectId: "my-gcp-project",
        unknownProperty: "some-value",
      }),
    );
    expect(() => credentials.read(credentialsPath)).to.throw(
      /Unexpected property "unknownProperty"/,
    );
  });

  test("write creates a credentials file that read parses back", () => {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");

    credentials.write(credentialsPath, { projectId: "my-gcp-project", location: "US" });

    expect(JSON.parse(fs.readFileSync(credentialsPath, "utf8"))).to.deep.equal({
      projectId: "my-gcp-project",
      location: "US",
    });
    const parsed = credentials.read(credentialsPath);
    expect(parsed.projectId).to.equal("my-gcp-project");
    expect(parsed.location).to.equal("US");
  });

  test("write replaces the contents of an existing credentials file", () => {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({ projectId: "old-gcp-project", location: "EU", credentials: "old-key" }),
    );

    credentials.write(credentialsPath, { projectId: "my-gcp-project" });

    expect(JSON.parse(fs.readFileSync(credentialsPath, "utf8"))).to.deep.equal({
      projectId: "my-gcp-project",
    });
  });

  // Permission bits are POSIX-only.
  if (process.platform !== "win32") {
    const permissionBits = (filePath: string) => fs.statSync(filePath).mode & 0o777;

    test("write creates the credentials file with owner-only permissions", () => {
      const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");

      credentials.write(credentialsPath, { projectId: "my-gcp-project" });

      expect(permissionBits(credentialsPath)).to.equal(0o600);
    });

    test("write restricts an existing credentials file to its owner", () => {
      const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");
      fs.writeFileSync(credentialsPath, "{}");
      // Set explicitly, so that the starting permissions don't depend on the umask.
      fs.chmodSync(credentialsPath, 0o644);

      credentials.write(credentialsPath, { projectId: "my-gcp-project" });

      expect(permissionBits(credentialsPath)).to.equal(0o600);
    });
  }
});
