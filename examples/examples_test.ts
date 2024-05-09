import { expect } from "chai";
import * as fs from "fs-extra";

import { execFile } from "child_process";
import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { getProcessResult, nodePath, suite, test } from "df/testing";

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
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(`${projectDir}/package.json`, "");

      // TODO(ekrekr): make this test `run` instead.
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
