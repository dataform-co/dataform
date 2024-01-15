import { expect } from "chai";
import * as fs from "fs-extra";
import { load as loadYaml } from "js-yaml";
import * as path from "path";

import { compile } from "df/cli/api";
import { version } from "df/common/version";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("examples", () => {
  test("ensure matching dataformCoreVersion", async () => {
    // To make examples code work without modification, dataformCoreVersion needs to be given. This
    // test ensures that the version doesn't get out of sync with that of current development.
    fs.readdirSync("examples", { withFileTypes: true })
      .filter(dir => dir.isDirectory() && dir.name !== "node_modules")
      .forEach(folder => {
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        const workflowSettingsYamlContents = fs.readFileSync(
          path.join("examples", folder.name, "workflow_settings.yaml"),
          "utf-8"
        );
        const projectConfig = dataform.ProjectConfig.create(loadYaml(workflowSettingsYamlContents));
        expect(projectConfig.dataformCoreVersion).equals(version);
      });
  });

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
