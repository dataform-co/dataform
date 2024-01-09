import { expect } from "chai";
import * as fs from "fs-extra";

import { compile } from "df/cli/api";
import { suite, test } from "df/testing";

suite("examples", () => {
  test("stackoverflow_reporter compiles", async () => {
    fs.copySync("examples/node_modules", "examples/stackoverflow_reporter/node_modules");

    const graph = await compile({ projectDir: "examples/stackoverflow_reporter" });

    expect(graph.graphErrors.compilationErrors.length).equals(0);
  });

  test("extreme_weather_programming compiles", async () => {
    fs.copySync("examples/node_modules", "examples/extreme_weather_programming/node_modules");

    const graph = await compile({ projectDir: "examples/extreme_weather_programming" });

    expect(graph.graphErrors.compilationErrors.length).equals(0);
    expect(graph.tables.length).equals(3);
    expect(graph.notebooks.length).equals(1);
  });
});
