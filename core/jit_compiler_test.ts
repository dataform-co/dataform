import { expect } from "chai";

import { jitCompile } from "df/core/jit_compiler";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("jit_compiler", () => {
  const rpcCallback: (method: string, request: Uint8Array, callback: (error: Error | null, response: Uint8Array) => void) => void =
    (method, request, callback) => { callback(null, new Uint8Array()); };

  const target = dataform.Target.create({
    database: "db",
    schema: "schema",
    name: "name"
  });

  suite("jitCompileOperation", () => {
    test("compiles operation returning string", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => "SELECT 1"`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation returning array", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ["SELECT 1", "SELECT 2"]`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1", "SELECT 2"]);
    });

    test("compiles operation returning object", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ queries: ["SELECT 1"] })`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation using context", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async (ctx) => ({ queries: [\`SELECT "\${ctx.name()}"\`] })`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(['SELECT "name"']);
    });

    test("compiles operation with arrow function returning promise", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `(ctx) => Promise.resolve("SELECT 1")`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation with async function", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `async function(ctx) { return "SELECT 1"; }`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.operation.queries).to.deep.equal(["SELECT 1"]);
    });

    test("compiles operation with regular function returning promise", async () => {
      const request = dataform.JitCompilationRequest.create({
        jitCode: `function(ctx) { return Promise.resolve("SELECT 1"); }`,
        target,
        jitData: {},
        compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
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
         compilationTargetType: dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_INCREMENTAL_TABLE,
      });
      const result = await jitCompile(request, rpcCallback);
      expect(result.incrementalTable.incremental?.query).to.equal("SELECT INC");
      expect(result.incrementalTable.regular?.query).to.equal("SELECT REG");
    });
  });
});
