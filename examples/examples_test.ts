import { expect } from "chai";

import { compile } from "df/api";
import { suite, test } from "df/testing";

suite("examples", () => {
  test("example Dataform projects compile", async () => {
    const graph = await compile({ projectDir: "examples/stackoverflow_reporter" });
    expect(graph.graphErrors.compilationErrors.length).equals(0);
  });
});
