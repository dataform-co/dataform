import { expect } from "chai";
import Long from "long";

import { Structs } from "df/common/protos/structs";
import { jitCompile } from "df/core/jit_compiler";
import { dataform } from "df/protos/ts";
import { suite, test, writeDefinitionFile, writeWorkflowSettingsFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

suite("jit_compiler", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const rpcCallback: (
    method: string,
    request: Uint8Array,
    callback: (error: Error | null, response: Uint8Array) => void,
  ) => void = (method, request, callback) => {
    callback(null, new Uint8Array());
  };

  const target = dataform.Target.create({
    database: "db",
    schema: "schema",
    name: "name",
  });

  suite("jitCompileOperation", () => {
    test("compiles operation returning string", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => "SELECT 1"`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation returning array", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ["SELECT 1", "SELECT 2"]`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1", "SELECT 2"]);
    });

    test("compiles operation returning object", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ queries: ["SELECT 1"] })`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation using context", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ queries: [\`SELECT "\${ctx.name()}"\`] })`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(['SELECT "name"']);
    });

    test("compiles operation with arrow function returning promise", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `(ctx) => Promise.resolve("SELECT 1")`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation with async function", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async function(ctx) { return "SELECT 1"; }`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation with regular function returning promise", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `function(ctx) { return Promise.resolve("SELECT 1"); }`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });
  });

  suite("jitCompileTable", () => {
    test("compiles table returning string", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => "SELECT 1"`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.table.query).to.equal("SELECT 1");
    });

    test("compiles table returning object", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ query: "SELECT 1", preOps: ["PRE"], postOps: ["POST"] })`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.table.query).to.equal("SELECT 1");
      expect(result.table.preOps).to.deep.equal(["PRE"]);
      expect(result.table.postOps).to.deep.equal(["POST"]);
    });
  });

  suite("jitCompileAssertion", () => {
    test("compiles assertion returning string", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => "SELECT * FROM t WHERE invalid"`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.assertion.query).to.equal("SELECT * FROM t WHERE invalid");
    });

    test("compiles assertion using context", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => \`SELECT * FROM \${ctx.ref('other')} WHERE invalid\``,
        target,
        jitData: {},
        dependencies: [
          dataform.Target.create({
            database: "db",
            schema: "schema",
            name: "other",
          }),
        ],
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.assertion.query).to.equal("SELECT * FROM `db.schema.other` WHERE invalid");
    });

    test("compiles assertion returning bare object", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ query: "SELECT * FROM t WHERE invalid" })`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.assertion.query).to.equal("SELECT * FROM t WHERE invalid");
    });

    test("compiles assertion returning object with extra fields", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ query: "SELECT * FROM t WHERE invalid", preOps: [], postOps: [] })`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.assertion.query).to.equal("SELECT * FROM t WHERE invalid");
    });
  });

  suite("jitCompileIncrementalTable", () => {
    test("compiles incremental table", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => {
          if (ctx.incremental()) {
            return { query: "SELECT INC" };
          }
          return { query: "SELECT REG" };
        }`,
        target,
        jitData: {},
        compilationTargetType:
          dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_INCREMENTAL_TABLE,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.incrementalTable.incremental?.query).to.equal("SELECT INC");
      expect(result.incrementalTable.regular?.query).to.equal("SELECT REG");
    });
  });

  suite("jitCompileContext", () => {
    test("can reference self and other tables", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (jctx) => \`$\{jctx.self()\}\n$\{jctx.ref('other')\}\``,
        target,
        jitData: {},
        dependencies: [
          dataform.Target.create({
            database: "db",
            schema: "schema",
            name: "other",
          }),
        ],
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.table.query).to.equal("`db.schema.name`\n`db.schema.other`");
    });

    test("can reference execution info data", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (jctx) => \`$\{jctx.executionData.executionStartTime.seconds\}\n$\{jctx.executionData.executionId\}\``,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE,
        executionData: {
          executionId: "test-id",
          executionStartTime: { seconds: Long.fromNumber(1774974514), nanos: 481 },
        },
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.table.query).to.equal("1774974514\ntest-id");
    });
  });

  suite("sqlx round-trip jit compilation", () => {
    test("compiles and executes table sqlx with adapter, data, executionData, ref, pre/post ops, and regex backslashes", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "setup_data.js",
        `const { session } = require("@dataform/core");
session.jitData("prefix", "col");`,
      );
      writeDefinitionFile(projectDir, "upstream.sqlx", `config { type: "table" } SELECT 1 AS id`);
      writeDefinitionFile(
        projectDir,
        "jit_table.sqlx",
        `config {
  type: "table",
  compilation_mode: "jit",
  dependencies: ["upstream"]
}
js {
  const execRes = await adapter.execute({ statement: "SELECT 'x' AS col_name" });
  const col = execRes.rows[0].fields.col_name.stringValue;
  const matched = "a_123".replace(/\\d+/, "N");
  const execId = executionData.executionId;
}
SELECT
  '\${data.prefix}_\${col}' AS dynamic_col,
  '\${matched}' AS regex_val,
  '\${execId}' AS exec_id,
  REGEXP_EXTRACT('abc_99', r'\\w+_\\d+') AS sql_regex
FROM \${ref("upstream")}
pre_operations {
  SELECT '\${self()}' AS pre_target
}
post_operations {
  SELECT '\${name()}' AS post_name
}`,
      );

      const coreResult = runMainInVm(coreExecutionRequestFromPath(projectDir));
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);

      const tableProto = coreResult.compile.compiledGraph.tables.find(
        (t) => t.target.name === "jit_table",
      );
      expect(tableProto).to.not.equal(undefined);

      const mockExecuteResponse = dataform.ExecuteResponse.encode({
        rows: [Structs.fromObject({ col_name: "dyn" })],
      }).finish();

      const jitResult = await jitCompile(
        dataform.JitCompilationRequest.create({
          jitCode: tableProto.jitCode,
          target: tableProto.target,
          dependencies: tableProto.dependencyTargets,
          jitData: coreResult.compile.compiledGraph.jitData,
          compilationTargetType:
            dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE,
          executionData: {
            executionId: "exec-42",
          },
        }),
        (_method, _req, cb) => cb(null, mockExecuteResponse),
      );

      expect(jitResult.table.query.trim()).to.equal(
        `SELECT
  'col_dyn' AS dynamic_col,
  'a_N' AS regex_val,
  'exec-42' AS exec_id,
  REGEXP_EXTRACT('abc_99', r'\\w+_\\d+') AS sql_regex
FROM \`defaultProject.defaultDataset.upstream\``,
      );
      expect(jitResult.table.preOps.map((s) => s.trim())).to.deep.equal([
        "SELECT '`defaultProject.defaultDataset.jit_table`' AS pre_target",
      ]);
      expect(jitResult.table.postOps.map((s) => s.trim())).to.deep.equal([
        "SELECT 'jit_table' AS post_name",
      ]);
    });

    test("compiles and executes incremental table sqlx for both regular and incremental paths", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "jit_inc.sqlx",
        `config {
  type: "incremental",
  compilation_mode: "jit"
}
SELECT 1 AS id
\${when(incremental(), \`WHERE id > (SELECT MAX(id) FROM \${self()})\`)}
pre_operations {
  SELECT '\${when(incremental(), "inc_pre", "full_pre")}'
}`,
      );

      const coreResult = runMainInVm(coreExecutionRequestFromPath(projectDir));
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const incProto = coreResult.compile.compiledGraph.tables[0];

      const jitResult = await jitCompile(
        dataform.JitCompilationRequest.create({
          jitCode: incProto.jitCode,
          target: incProto.target,
          dependencies: incProto.dependencyTargets,
          jitData: coreResult.compile.compiledGraph.jitData,
          compilationTargetType:
            dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_INCREMENTAL_TABLE,
        }),
        rpcCallback,
      );

      expect(jitResult.incrementalTable.regular.query.trim()).to.equal("SELECT 1 AS id");
      expect(jitResult.incrementalTable.regular.preOps.map((s) => s.trim())).to.deep.equal([
        "SELECT 'full_pre'",
      ]);
      expect(jitResult.incrementalTable.incremental.query.trim()).to.equal(
        "SELECT 1 AS id\nWHERE id > (SELECT MAX(id) FROM `defaultProject.defaultDataset.jit_inc`)",
      );
      expect(jitResult.incrementalTable.incremental.preOps.map((s) => s.trim())).to.deep.equal([
        "SELECT 'inc_pre'",
      ]);
    });

    test("compiles and executes operations sqlx with multiple statements", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "jit_ops.sqlx",
        `config {
  type: "operations",
  compilation_mode: "jit"
}
js {
  const suffix = "v1";
}
SELECT '\${suffix}' AS first_op;
---
SELECT '\${schema()}.\${name()}' AS second_op`,
      );

      const coreResult = runMainInVm(coreExecutionRequestFromPath(projectDir));
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const opProto = coreResult.compile.compiledGraph.operations[0];

      const jitResult = await jitCompile(
        dataform.JitCompilationRequest.create({
          jitCode: opProto.jitCode,
          target: opProto.target,
          dependencies: opProto.dependencyTargets,
          jitData: coreResult.compile.compiledGraph.jitData,
          compilationTargetType:
            dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
        }),
        rpcCallback,
      );

      expect(jitResult.operation.queries.map((q) => q.trim())).to.deep.equal([
        "SELECT 'v1' AS first_op;",
        "SELECT 'defaultDataset.jit_ops' AS second_op",
      ]);
    });

    test("compiles and executes assertion sqlx", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "source_tbl.sqlx", `config { type: "table" } SELECT 1 AS id`);
      writeDefinitionFile(
        projectDir,
        "jit_assert.sqlx",
        `config {
  type: "assertion",
  compilation_mode: "jit",
  dependencies: ["source_tbl"]
}
SELECT * FROM \${ref("source_tbl")} \${when(true, "WHERE id IS NULL")}`,
      );

      const coreResult = runMainInVm(coreExecutionRequestFromPath(projectDir));
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      const assertionProto = coreResult.compile.compiledGraph.assertions[0];

      const jitResult = await jitCompile(
        dataform.JitCompilationRequest.create({
          jitCode: assertionProto.jitCode,
          target: assertionProto.target,
          dependencies: assertionProto.dependencyTargets,
          jitData: coreResult.compile.compiledGraph.jitData,
          compilationTargetType:
            dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION,
        }),
        rpcCallback,
      );

      expect(jitResult.assertion.query.trim()).to.equal(
        "SELECT * FROM `defaultProject.defaultDataset.source_tbl` WHERE id IS NULL",
      );
    });

    test("surfaces compilation error in compiledGraph when jit sqlx has invalid blocks", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "bad_inc.sqlx",
        `config {
  type: "incremental",
  compilation_mode: "jit"
}
SELECT 1 AS id
incremental_where {
  id > 0
}`,
      );

      const coreResult = runMainInVm(coreExecutionRequestFromPath(projectDir));
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors.length).to.equal(1);
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors[0].fileName).to.equal(
        "definitions/bad_inc.sqlx",
      );
      expect(coreResult.compile.compiledGraph.graphErrors.compilationErrors[0].message).to.include(
        "incremental_where is not supported with JiT compilation.",
      );
    });
  });
});
