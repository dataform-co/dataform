import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";

import {
  credentialsFromServiceAccountJson,
  getBigQueryCredentials,
  ICredentialsPrompts,
} from "df/cli/credentials";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

/** Answers the selection questions and the text questions in the given order. */
function fakePrompts(selections: number[], answers: string[]): ICredentialsPrompts {
  return {
    selectionQuestion: () => selections.shift(),
    question: () => answers.shift(),
  };
}

suite("getBigQueryCredentials", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("ADC without a service account to impersonate", () => {
    expect(getBigQueryCredentials(fakePrompts([0, 0], ["my-project", ""]))).to.eql({
      projectId: "my-project",
      location: "US",
    });
  });

  test("ADC with a service account to impersonate", () => {
    const prompts = fakePrompts([1, 0], ["my-project", " sa@my-project.iam.gserviceaccount.com "]);
    expect(getBigQueryCredentials(prompts)).to.eql({
      projectId: "my-project",
      location: "EU",
      impersonateServiceAccount: "sa@my-project.iam.gserviceaccount.com",
    });
  });

  test("JSON key with a service account to impersonate", () => {
    const keyPath = path.join(tmpDirFixture.createNewTmpDir(), "key.json");
    const key = JSON.stringify({ type: "service_account", project_id: "key-project" });
    fs.writeFileSync(keyPath, key);
    const prompts = fakePrompts([0, 1], [keyPath, "sa@key-project.iam.gserviceaccount.com"]);
    expect(getBigQueryCredentials(prompts)).to.eql({
      projectId: "key-project",
      credentials: key,
      location: "US",
      impersonateServiceAccount: "sa@key-project.iam.gserviceaccount.com",
    });
  });

  test("JSON key copies universe_domain and can still impersonate a service account", () => {
    const keyPath = path.join(tmpDirFixture.createNewTmpDir(), "key.json");
    const key = JSON.stringify({
      type: "service_account",
      project_id: "key-project",
      universe_domain: "s3nsapis.fr",
    });
    fs.writeFileSync(keyPath, key);
    const prompts = fakePrompts([0, 1], [keyPath, "sa@key-project.iam.gserviceaccount.com"]);
    expect(getBigQueryCredentials(prompts)).to.eql({
      projectId: "key-project",
      credentials: key,
      location: "US",
      universeDomain: "s3nsapis.fr",
      impersonateServiceAccount: "sa@key-project.iam.gserviceaccount.com",
    });
  });
});

suite("credentialsFromServiceAccountJson", () => {
  function keyJson(overrides: object = {}): string {
    return JSON.stringify({
      type: "service_account",
      project_id: "my-project",
      private_key: "fake-key",
      ...overrides,
    });
  }

  test("copies universe_domain from the key so BigQuery matches google-auth", () => {
    const credentials = credentialsFromServiceAccountJson(
      keyJson({ universe_domain: "my-universe.example.com" }),
      "EU",
    );

    expect(credentials.projectId).to.equal("my-project");
    expect(credentials.location).to.equal("EU");
    expect(credentials.universeDomain).to.equal("my-universe.example.com");
    expect(JSON.parse(credentials.credentials).universe_domain).to.equal("my-universe.example.com");
  });

  test("omits universeDomain when the key has no universe_domain", () => {
    const credentials = credentialsFromServiceAccountJson(keyJson(), "US");

    expect(credentials).to.not.have.property("universeDomain");
  });

  test("omits universeDomain when universe_domain is blank", () => {
    const credentials = credentialsFromServiceAccountJson(keyJson({ universe_domain: "  " }), "US");

    expect(credentials).to.not.have.property("universeDomain");
  });
});
