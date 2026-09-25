import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { compile } from "df/cli/vm/compile";
import { handleJitRequest } from "df/cli/vm/jit_worker";
import { decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("cli/vm", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  // Allow require("@dataform/core") to resolve to the prebuilt core bundle in the test environment
  // tslint:disable-next-line: no-require-imports
  const Module = require("module");
  const origResolve = Module._resolveFilename;
  Module._resolveFilename = function (request: string, parent: any, isMain: boolean, options: any) {
    if (request === "@dataform/core") {
      return path.join(process.cwd(), "core", "node_modules", "@dataform", "core", "bundle.js");
    }
    return origResolve.apply(this, arguments);
  };

  test("compile() runs end-to-end against prebuilt @dataform/core bundle", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    // Copy built @dataform/core from Bazel runfiles into the project's node_modules.
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );

    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: test-project
defaultDataset: test-dataset
defaultLocation: US
`,
    );

    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "example.sqlx"),
      `
config {
  type: "table",
  name: "example"
}
SELECT 1 AS col
`,
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "actions.yaml"),
      `
actions:
  - notebook:
      filename: test_notebook.ipynb
`,
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "test_notebook.ipynb"),
      JSON.stringify({ cells: [] }),
    );

    const encodedResponse = compile({ projectDir });
    const response = decode64(dataform.CoreExecutionResponse, encodedResponse);

    expect(response.compile).to.be.an("object");
    expect(response.compile.compiledGraph).to.be.an("object");
    const tables = response.compile.compiledGraph.tables;
    expect(tables).to.have.lengthOf(1);
    expect(tables[0].target.name).to.equal("example");
    expect(tables[0].target.schema).to.equal("test-dataset");
    expect(tables[0].target.database).to.equal("test-project");

    const notebooks = response.compile.compiledGraph.notebooks;
    expect(notebooks).to.have.lengthOf(1);
    expect(notebooks[0].target.name).to.equal("test_notebook");
  });

  test("handleJitRequest compiles request with local core (hasProjectLocalCore = true)", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );

    const request = dataform.JitCompilationRequest.create({
      jitCode: `async (ctx) => "SELECT 1"`,
      target: {
        database: "db",
        schema: "schema",
        name: "test_op",
      },
      compilationTargetType:
        dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
    });

    const messages: any[] = [];
    const origSend = process.send;
    (process as any).send = (msg: any) => messages.push(msg);

    try {
      await handleJitRequest({ request, projectDir });
    } finally {
      (process as any).send = origSend;
    }

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_response");
    expect(messages[0].response.operation.queries).to.deep.equal(["SELECT 1"]);
  });

  test("handleJitRequest compiles request with fallback core (hasProjectLocalCore = false)", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    // Provide only package.json for @dataform/core without bundle.js so hasProjectLocalCore is false
    const coreDir = path.join(projectDir, "node_modules", "@dataform", "core");
    fs.mkdirSync(coreDir, { recursive: true });
    fs.writeFileSync(
      path.join(coreDir, "package.json"),
      JSON.stringify({ name: "@dataform/core", version: "3.0.0" }),
    );

    const request = dataform.JitCompilationRequest.create({
      jitCode: `async (ctx) => "SELECT 42"`,
      target: {
        database: "db",
        schema: "schema",
        name: "test_op2",
      },
      compilationTargetType:
        dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
    });

    const messages: any[] = [];
    const origSend = process.send;
    (process as any).send = (msg: any) => messages.push(msg);

    try {
      await handleJitRequest({ request, projectDir });
    } finally {
      (process as any).send = origSend;
    }

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_response");
    expect(messages[0].response.operation.queries).to.deep.equal(["SELECT 42"]);
  });
});
