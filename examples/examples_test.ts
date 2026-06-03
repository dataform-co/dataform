import { expect } from "chai";
import { execFile } from "child_process";
import * as fs from "fs-extra";

import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { getProcessResult, nodePath, suite, test } from "df/testing";

suite("examples", { parallel: true }, () => {
  const cliEntryPointPath = "examples/node_modules/@dataform/cli/bundle.js";

  ["stackoverflow_reporter", "extreme_weather_programming"].forEach(exampleProject => {
    test(`${exampleProject} runs`, async () => {
      // compile() calls realpath on projectDir, which would jump out of bazel's
      // symlinked runfiles tree and leave the project without its sibling
      // node_modules. Materialize a real copy (dereference symlinks) so the
      // project and node_modules below resolve under a single real path.
      const originalProjectDir = `examples/${exampleProject}`;
      const projectDir = `examples/${exampleProject}_copy`;
      fs.copySync(originalProjectDir, projectDir, { dereference: true });
      fs.copySync(
        "examples/node_modules/@dataform/core",
        `${projectDir}/node_modules/@dataform/core`
      );
      // A blank `package.json` makes no `dataformCoreVersion` in `workflow_settings.yaml` be OK.
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(`${projectDir}/package.json`, "");

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
