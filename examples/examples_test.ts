import { expect } from "chai";

import { compile } from "df/cli/api";
import { suite, test } from "df/testing";
import * as fs from "fs-extra";

suite("examples", () => {
  // test("stackoverflow_reporter compiles", async () => {
  //   const graph = await compile({ projectDir: "examples/stackoverflow_reporter" });
  //   expect(graph.graphErrors.compilationErrors.length).equals(0);
  // });

  test("snow_programming compiles", async () => {
    fs.readdir("examples/snow_programming/node_modules", (err, files) => {
      files.forEach(file => {
        console.log(file);
      });
    });

    const graph = await compile({ projectDir: "examples/snow_programming" });
    console.log("🚀 ~ file: examples_test.ts:21 ~ test ~ graph:", graph);
    expect(graph.graphErrors.compilationErrors.length).equals(0);
  });
});
