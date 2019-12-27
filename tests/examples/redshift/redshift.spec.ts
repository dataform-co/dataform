import * as dfapi from "@dataform/api";
import { expect } from "chai";

describe("@dataform/examples/redshift", () => {
  it("runs", async () => {
    const credentials = dfapi.credentials.read("redshift", "df/test_credentials/redshift.json");

    const compiledGraph = await dfapi.compile({
      projectDir: "df/examples/redshift"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    // Run the project.
    const buildResult = await dfapi.build(compiledGraph, {}, credentials);
    const runResult = await dfapi.run(buildResult, credentials).resultPromise();

    expect(runResult.ok).equals(true);
  });
});
