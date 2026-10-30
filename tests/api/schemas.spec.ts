import { config, expect } from "chai";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

import { Runner } from "df/cli/api";
import { buildDryRunValidationQueries, dryRunValidationQueryKey } from "df/cli/api/commands/run";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { wrapWithCreateSchemaIfNotExists } from "df/cli/api/dbadapters/tasks";
import { sleep, sleepUntil } from "df/common/promises";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

config.truncateThreshold = 0;

suite("schema creation", () => {
  const projectConfig: dataform.IProjectConfig = {
    warehouse: "bigquery",
    defaultDatabase: "db",
    defaultSchema: "new_ds",
    assertionSchema: "new_assertions",
  };

  const tableAction = (
    name: string,
    schema: string,
    statements: string[],
    dependencies: string[] = [],
  ): dataform.IExecutionAction => ({
    type: "table",
    tableType: "table",
    target: { database: "db", schema, name },
    tasks: statements.map((statement) => ({ type: "statement", statement })),
    dependencyTargets: dependencies.map((dependency) => ({
      database: "db",
      schema,
      name: dependency,
    })),
  });

  const wrapped = (schema: string, statement: string) =>
    wrapWithCreateSchemaIfNotExists("db", schema, statement);

  function mockDbAdapter(
    existingSchemas: string[][] = [["existing_ds"]],
    executeImpl: (statement: string) => Promise<any> = async () => ({ rows: [], metadata: {} }),
  ) {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const executedStatements: string[] = [];
    let schemasCall = when(mockedDbAdapter.schemas(anyString()));
    existingSchemas.forEach((schemas) => (schemasCall = schemasCall.thenResolve(schemas)));
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(async (statement: string) => {
      executedStatements.push(statement);
      return executeImpl(statement);
    });
    return { mockedDbAdapter, executedStatements };
  }

  test("creates missing datasets in the first job of the first action writing to them", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a", "SECOND TASK a"]),
        tableAction("b", "new_ds", ["CREATE TABLE b"], ["a"]),
        tableAction("c", "existing_ds", ["CREATE TABLE c"]),
        {
          type: "assertion",
          target: { database: "db", schema: "new_assertions", name: "d" },
          tasks: [
            { type: "statement", statement: "CREATE VIEW d" },
            { type: "assertion", statement: "SELECT SUM(1) FROM d" },
          ],
          dependencyTargets: [],
        },
        {
          type: "table",
          tableType: "table",
          target: { database: "disabled_db", schema: "disabled_ds", name: "disabled" },
          tasks: [],
          dependencyTargets: [],
        },
      ],
    });
    const { mockedDbAdapter, executedStatements } = mockDbAdapter();

    const result = await new Runner(instance(mockedDbAdapter), graph).execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.schemas("disabled_db")).never();
    expect(executedStatements).to.have.members([
      wrapped("new_ds", "CREATE TABLE a"),
      // The dataset was created by the first task, so later tasks aren't wrapped.
      "SECOND TASK a",
      // The dataset was created by "a", so "b" isn't wrapped.
      "CREATE TABLE b",
      // Existing datasets are never created.
      "CREATE TABLE c",
      wrapped("new_assertions", "CREATE VIEW d"),
      // Assertion tasks are never wrapped.
      "SELECT SUM(1) FROM d",
    ]);
    expect(executedStatements).to.have.length(6);
  });

  test("only one job at a time creates a missing dataset", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a"]),
        tableAction("b", "new_ds", ["CREATE TABLE b"]),
      ],
    });
    let aStarted = false;
    let releaseA = false;
    const { mockedDbAdapter, executedStatements } = mockDbAdapter(
      [["existing_ds"]],
      async (statement) => {
        if (statement === wrapped("new_ds", "CREATE TABLE a")) {
          aStarted = true;
          await sleepUntil(() => releaseA);
        }
        return { rows: [], metadata: {} };
      },
    );

    const execution = new Runner(instance(mockedDbAdapter), graph).execute().result();
    await sleepUntil(() => aStarted);
    // Give "b" the chance to start, which it mustn't while "a" is creating the dataset.
    await sleep(50);
    expect(executedStatements).deep.equals([wrapped("new_ds", "CREATE TABLE a")]);
    releaseA = true;
    const result = await execution;

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    // "a" created the dataset, so "b" isn't wrapped.
    expect(executedStatements).deep.equals([
      wrapped("new_ds", "CREATE TABLE a"),
      "CREATE TABLE b",
    ]);
  });

  test("the next action writing to the dataset creates it if the creating action failed", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a", "SECOND TASK a"]),
        tableAction("b", "new_ds", ["CREATE TABLE b"]),
      ],
    });
    let bStarted = false;
    let releaseB = false;
    let aFinished = false;
    const { mockedDbAdapter, executedStatements } = mockDbAdapter(
      [["existing_ds"]],
      async (statement) => {
        if (statement === wrapped("new_ds", "CREATE TABLE a")) {
          throw new Error("a failed");
        }
        if (statement === wrapped("new_ds", "CREATE TABLE b")) {
          bStarted = true;
          await sleepUntil(() => releaseB);
        }
        return { rows: [], metadata: {} };
      },
    );

    const runner = Runner.create(instance(mockedDbAdapter), graph, {}, 0);
    runner.onChange((runResult) => {
      const actionA = runResult.actions.find((action) => action.target.name === "a");
      if (actionA?.status === dataform.ActionResult.ExecutionStatus.FAILED) {
        aFinished = true;
      }
    });
    const execution = runner.execute().result();
    await sleepUntil(() => bStarted);
    // "SECOND TASK a" is skipped without waiting for "b" to create the dataset.
    await sleep(50);
    const aFinishedWhileBRunning = aFinished;
    releaseB = true;
    const result = await execution;

    expect(aFinishedWhileBRunning).equals(true);
    expect(
      result.actions.map((action) => [action.target.name, action.status]),
    ).to.have.deep.members([
      ["a", dataform.ActionResult.ExecutionStatus.FAILED],
      ["b", dataform.ActionResult.ExecutionStatus.SUCCESSFUL],
    ]);
    expect(executedStatements).deep.equals([
      wrapped("new_ds", "CREATE TABLE a"),
      wrapped("new_ds", "CREATE TABLE b"),
    ]);
  });

  test("wraps tasks produced by JiT compilation", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "operation",
          target: { database: "db", schema: "new_ds", name: "jit_op" },
          tasks: [],
          jitCode: "async () => ({ queries: ['JIT QUERY'] })",
          dependencyTargets: [],
        },
      ],
    });
    const { mockedDbAdapter, executedStatements } = mockDbAdapter();

    const result = await new Runner(instance(mockedDbAdapter), graph, {
      jitCompiler: async () =>
        dataform.JitCompilationResponse.create({ operation: { queries: ["JIT QUERY"] } }),
    })
      .execute()
      .result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    expect(executedStatements).deep.equals([wrapped("new_ds", "JIT QUERY")]);
  });

  test("in dry runs, wraps every action writing to a missing dataset and reports the original SQL", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a"]),
        tableAction("b", "new_ds", ["CREATE TABLE b"], ["a"]),
        tableAction("c", "existing_ds", ["CREATE TABLE c"]),
      ],
    });
    const { mockedDbAdapter, executedStatements } = mockDbAdapter();

    const result = await new Runner(instance(mockedDbAdapter), graph, {
      bigquery: { dryRun: true },
    })
      .execute()
      .result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    // Dry runs don't create the dataset, so "b" is wrapped too.
    expect(executedStatements).to.have.members([
      wrapped("new_ds", "CREATE TABLE a"),
      wrapped("new_ds", "CREATE TABLE b"),
      "CREATE TABLE c",
    ]);
    expect(
      result.actions.map((action) => [action.target.name, action.tasks[0].compiledSql]),
    ).to.have.deep.members([
      ["a", "CREATE TABLE a"],
      ["b", "CREATE TABLE b"],
      ["c", "CREATE TABLE c"],
    ]);
  });

  test("in dry runs, actions writing to the same missing dataset run concurrently", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a"]),
        tableAction("b", "new_ds", ["CREATE TABLE b"]),
      ],
    });
    let bStarted = false;
    const { mockedDbAdapter, executedStatements } = mockDbAdapter(
      [["existing_ds"]],
      async (statement) => {
        if (statement === wrapped("new_ds", "CREATE TABLE a")) {
          // Would never finish if "b" waited for "a".
          await sleepUntil(() => bStarted);
        }
        if (statement === wrapped("new_ds", "CREATE TABLE b")) {
          bStarted = true;
        }
        return { rows: [], metadata: {} };
      },
    );

    const result = await new Runner(instance(mockedDbAdapter), graph, {
      bigquery: { dryRun: true },
    })
      .execute()
      .result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    expect(executedStatements).to.have.members([
      wrapped("new_ds", "CREATE TABLE a"),
      wrapped("new_ds", "CREATE TABLE b"),
    ]);
  });

  suite("dry-run validation queries", () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        tableAction("a", "new_ds", ["CREATE TABLE a AS SELECT a"]),
        tableAction("c", "existing_ds", ["CREATE TABLE c AS SELECT c"]),
        {
          type: "operation",
          target: { database: "db", schema: "new_ds", name: "op" },
          tasks: [{ type: "statement", statement: "OPERATION" }],
          dependencyTargets: [],
        },
      ],
    });
    const dryRunValidationQueries = new Map([
      [dryRunValidationQueryKey({ database: "db", schema: "new_ds", name: "a" }), "SELECT a"],
      [
        dryRunValidationQueryKey({ database: "db", schema: "existing_ds", name: "c" }),
        "SELECT c",
      ],
    ]);

    test("dry-runs the query of actions writing to a missing dataset", async () => {
      const { mockedDbAdapter, executedStatements } = mockDbAdapter();

      const result = await new Runner(instance(mockedDbAdapter), graph, {
        bigquery: { dryRun: true },
        dryRunValidationQueries,
      })
        .execute()
        .result();

      expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
      expect(executedStatements).to.have.members([
        wrapped("new_ds", "CREATE TABLE a AS SELECT a"),
        "SELECT a",
        // Not wrapped, so fully checked already.
        "CREATE TABLE c AS SELECT c",
        // No query to validate separately.
        wrapped("new_ds", "OPERATION"),
      ]);
      expect(executedStatements).to.have.length(4);
    });

    test("reports a failure of the query on the action's task after retries", async () => {
      let attempts = 0;
      const { mockedDbAdapter } = mockDbAdapter([["existing_ds"]], async (statement) => {
        if (statement === "SELECT a") {
          attempts++;
          throw new Error("Unrecognized name: a");
        }
        return { rows: [], metadata: {} };
      });

      const result = await new Runner(instance(mockedDbAdapter), graph, {
        bigquery: { dryRun: true, actionRetryLimit: 1 },
        dryRunValidationQueries,
      })
        .execute()
        .result();

      expect(attempts).equals(2);
      expect(result.status).equals(dataform.RunResult.ExecutionStatus.FAILED);
      const actionA = result.actions.find((action) => action.target.name === "a");
      expect(actionA.status).equals(dataform.ActionResult.ExecutionStatus.FAILED);
      expect(actionA.tasks).to.have.length(1);
      expect(actionA.tasks[0].status).equals(dataform.TaskResult.ExecutionStatus.FAILED);
      expect(actionA.tasks[0].errorMessage).equals(
        "bigquery error: Unrecognized name: a (in the query, validated separately because " +
          "dataset db.new_ds doesn't exist yet)",
      );
      expect(actionA.tasks[0].compiledSql).equals("CREATE TABLE a AS SELECT a");
    });

    test("doesn't dry-run the query if the wrapped script fails", async () => {
      const { mockedDbAdapter, executedStatements } = mockDbAdapter(
        [["existing_ds"]],
        async (statement) => {
          if (statement === wrapped("new_ds", "CREATE TABLE a AS SELECT a")) {
            throw new Error("Syntax error");
          }
          return { rows: [], metadata: {} };
        },
      );

      await new Runner(instance(mockedDbAdapter), graph, {
        bigquery: { dryRun: true },
        dryRunValidationQueries,
      })
        .execute()
        .result();

      expect(executedStatements).not.to.include("SELECT a");
    });

    test("are ignored outside of dry runs", async () => {
      const { mockedDbAdapter, executedStatements } = mockDbAdapter();

      await new Runner(instance(mockedDbAdapter), graph, { dryRunValidationQueries })
        .execute()
        .result();

      expect(executedStatements).not.to.include("SELECT a");
    });

    test("come from the JiT response for JiT actions", async () => {
      const jitGraph = dataform.ExecutionGraph.create({
        projectConfig,
        runConfig: {},
        warehouseState: {},
        actions: [
          {
            type: "table",
            tableType: "table",
            target: { database: "db", schema: "new_ds", name: "jit_table" },
            tasks: [],
            jitCode: "async () => 'SELECT jit'",
            dependencyTargets: [],
          },
        ],
      });
      const { mockedDbAdapter, executedStatements } = mockDbAdapter();

      const result = await new Runner(instance(mockedDbAdapter), jitGraph, {
        bigquery: { dryRun: true },
        jitCompiler: async () =>
          dataform.JitCompilationResponse.create({
            table: {
              preOps: ["DECLARE x INT64 DEFAULT 1;"],
              query: "SELECT x",
              postOps: ["SET x = 2;"],
            },
          }),
      })
        .execute()
        .result();

      expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
      expect(executedStatements).to.have.length(2);
      expect(executedStatements[1]).equals("DECLARE x INT64 DEFAULT 1;\nSELECT x;\nSET x = 2;");
    });

    test("are built from the tables and assertions of the compiled graph", () => {
      const queries = buildDryRunValidationQueries(
        dataform.CompiledGraph.create({
          tables: [
            { target: { database: "db", schema: "ds", name: "table" }, query: "SELECT 1" },
            {
              target: { database: "db", schema: "ds", name: "with_ops" },
              preOps: ["DECLARE x INT64 DEFAULT 1"],
              query: "SELECT x",
              postOps: ["SET x = 2"],
            },
            { target: { database: "db", schema: "ds", name: "jit" }, jitCode: "async () => ''" },
            {
              target: { database: "db", schema: "ds", name: "disabled" },
              query: "SELECT 2",
              disabled: true,
            },
          ],
          assertions: [
            { target: { database: "db", schema: "ds_assertions", name: "a" }, query: "SELECT 3" },
          ],
          operations: [
            { target: { database: "db", schema: "ds", name: "op" }, queries: ["SELECT 4"] },
          ],
        }),
      );

      expect(Array.from(queries.entries())).to.have.deep.members([
        [dryRunValidationQueryKey({ database: "db", schema: "ds", name: "table" }), "SELECT 1"],
        [
          dryRunValidationQueryKey({ database: "db", schema: "ds", name: "with_ops" }),
          "DECLARE x INT64 DEFAULT 1;\nSELECT x;\nSET x = 2",
        ],
        [
          dryRunValidationQueryKey({ database: "db", schema: "ds_assertions", name: "a" }),
          "SELECT 3",
        ],
      ]);
    });
  });

  suite("resume", () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [tableAction("a", "new_ds", ["CREATE TABLE a", "SECOND TASK a"])],
    });

    async function runUntilFirstTaskThenStop() {
      let firstTaskInProgress = false;
      let stopWasCalled = false;
      const { mockedDbAdapter } = mockDbAdapter([[]], async (statement) => {
        if (statement === wrapped("new_ds", "CREATE TABLE a")) {
          firstTaskInProgress = true;
          await sleepUntil(() => stopWasCalled);
        }
        return { rows: [], metadata: {} };
      });
      const runner = new Runner(instance(mockedDbAdapter), graph);
      runner.execute();
      await sleepUntil(() => firstTaskInProgress);
      runner.stop();
      stopWasCalled = true;
      return await runner.result();
    }

    test("wraps the first not-yet-executed task if the dataset is still missing", async () => {
      const partialResult = await runUntilFirstTaskThenStop();

      // E.g. the first job was interrupted before creating the dataset.
      const { mockedDbAdapter, executedStatements } = mockDbAdapter([[]]);
      const result = await Runner.resume(instance(mockedDbAdapter), graph, partialResult)
        .execute()
        .result();

      expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
      expect(executedStatements).deep.equals([wrapped("new_ds", "SECOND TASK a")]);
    });

    test("doesn't wrap tasks writing to datasets created before the interruption", async () => {
      const partialResult = await runUntilFirstTaskThenStop();

      const { mockedDbAdapter, executedStatements } = mockDbAdapter([["new_ds"]]);
      await Runner.resume(instance(mockedDbAdapter), graph, partialResult).execute().result();

      expect(executedStatements).deep.equals(["SECOND TASK a"]);
    });
  });
});
