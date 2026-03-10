import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { execFile } from "child_process";
import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { getProcessResult, nodePath, suite, test } from "df/testing";

suite("examples", { parallel: true }, () => {
  const cliEntryPointPath = "examples/node_modules/@dataform/cli/bundle.js";

  const examples = [
    {
      name: "stackoverflow_reporter"
    },
    {
      name: "extreme_weather_programming"
    },
    {
      name: "extreme_weather_jit_showcase",
      additionalArgs: [
        "--vars=environment=test,metrics=snow,tornado,tempUnit=C,tempThreshold=25,groupByMonth=true"
      ]
    }
  ];

  examples.forEach(example => {
    test(`${example.name} runs`, async () => {
      const projectDir = `examples/${example.name}`;

      setupProject(projectDir);

      const compiledGraph = await runCompile(projectDir, example.additionalArgs || []);

      expect(compiledGraph.graphErrors).deep.equals({});

      assertGolden(example.name, compiledGraph);
    });
  });

  function setupProject(projectDir: string) {
    fs.copySync("examples/node_modules/@dataform/core", `${projectDir}/node_modules/@dataform/core`);
    // A blank `package.json` makes no `dataformCoreVersion` in `workflow_settings.yaml` be OK.
    // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
    fs.writeFileSync(`${projectDir}/package.json`, "");
  }

  async function runCompile(
    projectDir: string,
    additionalArgs: string[]
  ): Promise<dataform.ICompiledGraph> {
    const processResult = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, ...additionalArgs, "--json"])
    );

    expect(processResult.exitCode).equals(0);

    return JSON.parse(
      JSON.stringify(
        verifyObjectMatchesProto(dataform.CompiledGraph, JSON.parse(processResult.stdout))
      )
    );
  }

  function assertGolden(exampleName: string, actualGraph: any) {
    const goldenPath = `examples/expected/${exampleName}.json`;
    const expectedGraph = fs.readJsonSync(goldenPath);
    expect(actualGraph).to.deep.equal(expectedGraph);
  }
});
