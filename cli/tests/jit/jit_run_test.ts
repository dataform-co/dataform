import { expect } from "chai";
import { anything, capture, instance, mock, verify, when } from "ts-mockito";

import { handleDbRequest as handleRpc } from "df/cli/api/commands/jit/rpc";
import { Runner } from "df/cli/api/commands/run";
import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { jitCompile } from "df/core/jit_compiler";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("run", () => {
  test("JiT compilation is performed for Table actions", async () => {
    const { mockAdapter, adapterInstance } = createMocks();

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "jit_table" },
        type: "table",
        tableType: "table",
        jitCode: "async (jctx) => { return 'SELECT 1'; }",
        tasks: []
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          // RPC callback bridge for tests
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    // Verify overall run status
    if (result.status !== dataform.RunResult.ExecutionStatus.SUCCESSFUL) {
      process.stderr.write("Run failed with actions: " + JSON.stringify(result.actions, null, 2) + "\n");
    }
    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Verify action result
    const actionResult = result.actions[0];
    expect(actionResult.target.name).equals("jit_table");
    expect(actionResult.status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

    // Verify task results
    expect(actionResult.tasks.length).equals(1);
    expect(actionResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.SUCCESSFUL);

    // Verify that the Runner executed the query statement returned by JiT compilation
    verify(mockAdapter.execute(anything(), anything())).atLeast(1);
  });

  test("JiT compilation is performed for Operation actions", async () => {
    const { mockAdapter, adapterInstance } = createMocks();

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "jit_op" },
        type: "operation",
        jitCode: "async (jctx) => { return ['SELECT 1', 'SELECT 2']; }",
        tasks: []
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          // RPC callback bridge for tests
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    const actionResult = result.actions[0];
    expect(actionResult.status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);
    expect(actionResult.tasks.length).equals(2);
    expect(actionResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.SUCCESSFUL);
    expect(actionResult.tasks[1].status).equals(dataform.TaskResult.ExecutionStatus.SUCCESSFUL);

    verify(mockAdapter.execute("SELECT 1", anything())).once();
    verify(mockAdapter.execute("SELECT 2", anything())).once();
  });

  test("Mixed run with JiT and AoT actions", async () => {
    const { mockAdapter, adapterInstance } = createMocks();

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "aot_table" },
        type: "table",
        tableType: "table",
        tasks: [dataform.ExecutionTask.create({ statement: "SELECT 'aot'", type: "statement" })]
      },
      {
        target: { database: "db", schema: "sch", name: "jit_table" },
        type: "table",
        tableType: "table",
        jitCode: "async (jctx) => { return 'SELECT \"jit\"'; }",
        tasks: [],
        dependencyTargets: [{ database: "db", schema: "sch", name: "aot_table" }]
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          // RPC callback bridge for tests
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    expect(result.actions.length).equals(2);

    const aotResult = result.actions.find((a: dataform.IActionResult) => a.target.name === "aot_table");
    const jitResult = result.actions.find((a: dataform.IActionResult) => a.target.name === "jit_table");

    expect(aotResult.status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);
    expect(aotResult.tasks.length).equals(1);
    expect(aotResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.SUCCESSFUL);

    expect(jitResult.status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);
    expect(jitResult.tasks.length).equals(1);
    expect(jitResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.SUCCESSFUL);

    // Verify that both actions resulted in database execution calls
    verify(mockAdapter.execute(anything(), anything())).atLeast(2);
    const [firstStatement] = capture(mockAdapter.execute).first();
    const [secondStatement] = capture(mockAdapter.execute).second();
    const allStatements = [firstStatement, secondStatement];
    expect(allStatements.some((s: string) => s.includes("SELECT 'aot'"))).to.equal(true);
    expect(allStatements.some((s: string) => s.includes("SELECT \"jit\""))).to.equal(true);
  });

  test("Handles JiT compilation syntax error", async () => {
    const { adapterInstance } = createMocks();

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "bad_jit" },
        type: "table",
        tableType: "table",
        jitCode: "async (jctx) => { return syntax error; }",
        tasks: []
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          // RPC callback bridge for tests
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.FAILED);

    const actionResult = result.actions[0];
    expect(actionResult.status).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(actionResult.tasks.length).equals(1);
    expect(actionResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.FAILED);
    expect(actionResult.tasks[0].errorMessage).to.contain("JiT compilation error");
  });

  test("Handles database error during JiT compilation (RPC failure)", async () => {
    const { adapterInstance } = createMocks();

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "jit_db_error" },
        type: "table",
        tableType: "table",
        // This code calls jctx.adapter.execute() which triggers our mockClient.execute
        jitCode: "async (jctx) => { await jctx.adapter.execute({statement: 'SELECT fail'}); return 'SELECT 2'; }",
        tasks: []
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          // RPC callback bridge for tests
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });

    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.FAILED);

    const actionResult = result.actions[0];
    expect(actionResult.status).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(actionResult.tasks.length).equals(1);
    expect(actionResult.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.FAILED);
    expect(actionResult.tasks[0].errorMessage).to.contain("RPC DB Fail");
  });

  test("Handles JiT incremental table compilation", async () => {
    const target = { database: "db", schema: "sch", name: "incremental_jit" };
    const executionGraph = createGraph([
      {
        target,
        type: "table",
        tableType: "incremental",
        jitCode: `async (jctx) => {
          return jctx.incremental() ? "SELECT 'inc' as t" : "SELECT 'full' as t";
        }`,
        tasks: []
      }
    ]);

    let runner: Runner;

    // 1. First run - empty warehouse, should use 'full' path
    const { mockAdapter: mockAdapterFull, adapterInstance: adapterInstanceFull } = createMocks();
    runner = new Runner(adapterInstanceFull, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const fullResult = await runner.execute().result();
    expect(fullResult.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    verify(mockAdapterFull.execute(anything(), anything())).atLeast(1);
    const [executedSqlFull] = capture(mockAdapterFull.execute).last();
    expect(executedSqlFull).to.contain("create or replace table `db.sch.incremental_jit` as");
    expect(executedSqlFull).to.contain("SELECT 'full' as t");

    // 2. Mock that the table now exists in the warehouse
    executionGraph.warehouseState.tables.push({
      target,
      type: dataform.TableMetadata.Type.TABLE,
      fields: [{ name: "t" }]
    });

    // 3. Second run - table exists, should use 'incremental' path
    const {
      mockAdapter: mockAdapterIncremental,
      adapterInstance: adapterInstanceIncremental
    } = createMocks();
    runner = new Runner(adapterInstanceIncremental, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const incrementalResult = await runner.execute().result();
    expect(incrementalResult.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    verify(mockAdapterIncremental.execute(anything(), anything())).atLeast(1);
    const [executedSqlIncremental] = capture(mockAdapterIncremental.execute).last();
    expect(executedSqlIncremental).to.contain("SELECT 'inc' as t");
  });

  test("Handles JiT incremental table compilation - incremental mode", async () => {
    const { mockAdapter, adapterInstance } = createMocks();

    const target = { database: "db", schema: "sch", name: "incremental_jit" };
    const executionGraph = createGraph([
      {
        target,
        type: "table",
        tableType: "incremental",
        jitCode: `async (jctx) => {
          return jctx.incremental() ? "SELECT 'inc' as t" : "SELECT 'full' as t";
        }`,
        tasks: []
      }
    ]);
    // Mock that the table already exists in the warehouse as a TABLE with a 't' field
    executionGraph.warehouseState.tables.push({
      target,
      type: dataform.TableMetadata.Type.TABLE,
      fields: [{ name: "t" }]
    });

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Verify it used the 'incremental' query path
    verify(mockAdapter.execute(anything(), anything())).atLeast(1);
    const [executedSql] = capture(mockAdapter.execute).last();
    // For BigQuery, it should be an 'insert into' because no uniqueKey was specified.
    // We check for substrings without trailing spaces to avoid exact whitespace mismatches.
    // tslint:disable: tsr-detect-sql-literal-injection
    expect(executedSql).to.equal(
      "insert into `db.sch.incremental_jit`	\n" +
      "(`t`)	\n" +
      "select `t`	\n" +
      "from (SELECT 'inc' as t) as insertions"
    );
    // tslint:enable: tsr-detect-sql-literal-injection
  });

  test("JiT compilation with RPC calls (ListTables, GetTable, DeleteTable)", async () => {
    const { mockAdapter, adapterInstance } = createMocks();

    const target = { database: "db", schema: "sch", name: "existing_table" };
    when(mockAdapter.tables(anything(), anything())).thenResolve([{ target }]);
    when(mockAdapter.table(anything())).thenResolve({
      target,
      type: dataform.TableMetadata.Type.TABLE
    } as any);

    const executionGraph = createGraph([
      {
        target: { database: "db", schema: "sch", name: "jit_rpc_test" },
        type: "table",
        tableType: "table",
        jitCode: `async (jctx) => {
          const list = await jctx.adapter.listTables({ database: "db", schema: "sch" });
          const table = await jctx.adapter.getTable({ target: list.tables[0].target });
          await jctx.adapter.deleteTable({ target: table.target });
          return "SELECT '" + table.target.name + "' as deleted_table";
        }`,
        tasks: []
      }
    ]);

    const runner = new Runner(adapterInstance, executionGraph, {
      jitCompiler: async (req, pdir, adapter) => {
        return await jitCompile(req, (method, internalReq, callback) => {
          (adapter as any).rpcImpl(method, internalReq, callback);
        });
      }
    });
    const result = await runner.execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    const actionResult = result.actions[0];
    expect(actionResult.status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

    verify(mockAdapter.deleteTable(anything())).once();
    const [deletedTarget] = capture(mockAdapter.deleteTable).last();
    expect(deletedTarget.name).equals("existing_table");

    verify(mockAdapter.execute(anything(), anything())).once();
    const [executedSql] = capture(mockAdapter.execute).last();
    expect(executedSql).to.contain("SELECT 'existing_table' as deleted_table");
  });
});

function createMocks() {
  const mockAdapter = mock<IDbAdapter>();
  const mockClient = mock<IDbClient>();

  when(mockAdapter.schemas(anything())).thenResolve([]);
  when(mockAdapter.execute(anything(), anything())).thenCall((statement: string) => {
    if (statement.includes("fail") || statement.includes("nonexistent")) {
      throw new Error("RPC DB Fail");
    }
    return Promise.resolve({
      rows: [],
      metadata: {}
    });
  });
  when(mockClient.executeRaw(anything(), anything())).thenCall((statement: string) => {
    if (statement.includes("fail") || statement.includes("nonexistent")) {
      throw new Error("RPC DB Fail");
    }
    return Promise.resolve({
      rows: [],
      metadata: {}
    });
  });
  when(mockClient.execute(anything(), anything())).thenCall((statement: string) => {
    if (statement.includes("fail") || statement.includes("nonexistent")) {
      throw new Error("RPC DB Fail");
    }
    return Promise.resolve({
      rows: [],
      metadata: {}
    });
  });

  const adapterInstance = instance(mockAdapter);
  (adapterInstance as any).rpcImpl = (method: string, req: Uint8Array, callback: any) => {
    handleRpc(instance(mockAdapter), instance(mockClient), method, req)
      .then((res: Uint8Array) => callback(null, res))
      .catch((err: Error) => callback(err, null));
  };

  return { mockAdapter, mockClient, adapterInstance };
}

function createGraph(actions: any[]): dataform.ExecutionGraph {
  return dataform.ExecutionGraph.create({
    projectConfig: { warehouse: "bigquery" },
    runConfig: { fullRefresh: false, timeoutMillis: 30000 },
    warehouseState: { tables: [] },
    actions: actions.map(a => ({
      dependencyTargets: [],
      ...a
    }))
  });
}
