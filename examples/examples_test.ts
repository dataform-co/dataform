import { expect } from "chai";
import * as fs from "fs-extra";

import { compile } from "df/cli/api";
import { suite, test } from "df/testing";

suite("examples", () => {
  test("stackoverflow_reporter compiles", async () => {
    fs.copySync("examples/node_modules", "examples/stackoverflow_reporter/node_modules");
    fs.writeFileSync("examples/stackoverflow_reporter/package.json", "");

    const graph = await compile({ projectDir: "examples/stackoverflow_reporter" });

    expect(graph.graphErrors.compilationErrors).deep.equals([]);
  });

  test("extreme_weather_programming compiles", async () => {
    fs.copySync("examples/node_modules", "examples/extreme_weather_programming/node_modules");
    fs.writeFileSync("examples/extreme_weather_programming/package.json", "");

    const graph = await compile({ projectDir: "examples/extreme_weather_programming" });

    expect(graph.graphErrors.compilationErrors).deep.equals([]);
    expect(graph.tables.length).equals(3);
    expect(graph.notebooks.length).equals(1);
  });
});
