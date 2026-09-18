import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { read } from "df/cli/api/commands/credentials";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("credentials", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  function writeCredentials(contents: object): string {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), ".df-credentials.json");
    fs.writeFileSync(credentialsPath, JSON.stringify(contents));
    return credentialsPath;
  }

  test("read maps universeDomain when present", () => {
    const credentialsPath = writeCredentials({
      projectId: "my-project",
      location: "US",
      universeDomain: "my-universe.example.com"
    });

    const credentials = read(credentialsPath);

    expect(credentials.projectId).to.equal("my-project");
    expect(credentials.location).to.equal("US");
    expect(credentials.universeDomain).to.equal("my-universe.example.com");
  });

  test("read leaves universeDomain unset when omitted", () => {
    const credentialsPath = writeCredentials({ projectId: "my-project", location: "US" });

    const credentials = read(credentialsPath);

    expect(credentials.universeDomain).to.satisfy(
      (value: string) => value === "" || value === undefined
    );
  });

  test("read rejects unknown fields", () => {
    const credentialsPath = writeCredentials({ projectId: "my-project", notARealField: "x" });

    expect(() => read(credentialsPath)).to.throw(/notARealField/);
  });
});
