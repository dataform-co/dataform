import { expect } from "chai";

import { credentialsFromServiceAccountJson } from "df/cli/credentials";
import { suite, test } from "df/testing";

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
