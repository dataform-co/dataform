import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { suite, test, getProcessResult, nodePath } from "df/testing";
import { exec, execFile } from "child_process";
import { dataform } from "df/protos/ts";
import { verifyObjectMatchesProto } from "df/common/protos";

suite("examples", { parallel: true }, () => {
  const cliEntryPointPath = "examples/node_modules/@dataform/cli/bundle.js";

  ["stackoverflow_reporter", "extreme_weather_programming"].forEach(exampleProject => {
    test(`${exampleProject} runs`, async () => {
      const projectDir = `examples/${exampleProject}`;
      fs.copySync(
        "examples/node_modules/@dataform/core",
        `${projectDir}/node_modules/@dataform/core`
      );
      // A blank `package.json` makes no `dataformCoreVersion` in `workflow_settings.yaml` be OK.
      fs.writeFileSync(`${projectDir}/package.json`, "");

      // TODO(ekrekr): update this to test `run` instead, once viable.
      const processResult = await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"])
      );

      expect(processResult.exitCode).equals(0);
      const compiledGraph = verifyObjectMatchesProto(
        dataform.CompiledGraph,
        JSON.parse(processResult.stdout)
      );
      expect(compiledGraph.graphErrors).deep.equals({});
    });
  });
});
