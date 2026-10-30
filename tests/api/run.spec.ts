import { config, expect } from "chai";
import Long from "long";
import { anyString, anything, capture, instance, mock, verify, when } from "ts-mockito";

import { Runner } from "df/cli/api";
import { buildDryRunValidationQueries, dryRunValidationQueryKey } from "df/cli/api/commands/run";
import { IDbAdapter } from "df/cli/api/dbadapters";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { wrapWithCreateSchemaIfNotExists } from "df/cli/api/dbadapters/tasks";
import { sleep, sleepUntil } from "df/common/promises";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

config.truncateThreshold = 0;

suite("@dataform/api/run", () => {
  const RUN_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
    projectConfig: {
      warehouse: "bigquery",
      defaultSchema: "foo",
      assertionSchema: "bar",
      defaultDatabase: "database",
      defaultLocation: "US",
    },
    runConfig: {
      fullRefresh: true,
    },
    warehouseState: {
      tables: [
        {
          type: dataform.TableMetadata.Type.TABLE,
          target: {
            schema: "schema1",
            name: "target1",
          },
        },
      ],
    },
    actions: [
      {
        tasks: [
          {
            type: "executionTaskType",
            statement: "SELECT foo FROM bar",
          },
          {
            type: "executionTaskType",
            statement: "SELECT 42",
          },
        ],
        type: "table",
        target: {
          schema: "schema1",
          name: "target1",
        },
        tableType: "someTableType",
        dependencyTargets: [],
      },
      {
        tasks: [
          {
            type: "executionTaskType2",
            statement: "SELECT bar FROM baz",
          },
        ],
        type: "assertion",
        target: {
          database: "database2",
          schema: "schema2",
          name: "target2",
        },
        tableType: "someTableType",
        dependencyTargets: [
          {
            schema: "schema1",
            name: "target1",
          },
        ],
      },
    ],
  });

  const EXPECTED_RUN_RESULT = dataform.RunResult.create({
    status: dataform.RunResult.ExecutionStatus.FAILED,
    actions: [
      {
        target: RUN_TEST_GRAPH.actions![0].target,

        tasks: [
          {
            status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
            metadata: {
              bigquery: {
                jobId: "abc",
                totalBytesBilled: Long.fromNumber(0),
                totalBytesProcessed: Long.fromNumber(0),
              },
            },
          },
          {
            status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
            metadata: {},
          },
        ],
        status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      },
      {
        target: RUN_TEST_GRAPH.actions![1].target,
        tasks: [
          {
            status: dataform.TaskResult.ExecutionStatus.FAILED,
            metadata: {},
            errorMessage: "bigquery error: bad statement",
          },
        ],
        status: dataform.ActionResult.ExecutionStatus.FAILED,
      },
    ],
  });

  test("execute", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
    ).thenResolve({
      rows: [],
      metadata: {
        bigquery: {
          jobId: "abc",
          totalBytesBilled: Long.fromNumber(0),
          totalBytesProcessed: Long.fromNumber(0),
        },
      },
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
    ).thenResolve({
      rows: [],
      metadata: {},
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![1].tasks![0].statement!, anything()),
    ).thenReject(new Error("bad statement"));

    const mockDbAdapterInstance = instance(mockedDbAdapter);

    const runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
    ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
  });

  test("execute passes projectConfig.defaultLocation to BigQuery jobs, including the ones creating datasets", async () => {
    const graph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultDatabase: "db",
        defaultLocation: "europe-west1",
      },
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          tasks: [{ type: "statement", statement: "SELECT 1" }],
          type: "table",
          target: { database: "db", schema: "s", name: "t" },
          tableType: "table",
          dependencyTargets: [],
        },
      ],
    });
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.schemas(anyString())).thenResolve([]);
    when(mockedDbAdapter.execute(anyString(), anything())).thenResolve({ rows: [], metadata: {} });

    await new Runner(instance(mockedDbAdapter), graph).execute().result();

    const [statement, options] = capture(mockedDbAdapter.execute).last();
    expect(statement).to.equal(wrapWithCreateSchemaIfNotExists("db", "s", "SELECT 1"));
    expect(options.bigquery?.location).to.equal("europe-west1");
  });

  test("stop and then resume", async () => {
    let firstQueryInProgress = false;
    let stopWasCalled = false;

    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
    ).thenCall(async () => {
      firstQueryInProgress = true;
      await sleepUntil(() => stopWasCalled);
      return {
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0),
          },
        },
      };
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
    ).thenResolve({
      rows: [],
      metadata: {},
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![1].tasks![0].statement!, anything()),
    ).thenReject(new Error("bad statement"));

    const mockDbAdapterInstance = instance(mockedDbAdapter);

    let runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH);
    runner.execute();
    await sleepUntil(() => firstQueryInProgress);
    runner.stop();
    stopWasCalled = true;
    const result = cleanTiming(await runner.result());

    expect(dataform.RunResult.create(result).toJSON()).to.deep.equal(
      dataform.RunResult.create({
        status: dataform.RunResult.ExecutionStatus.RUNNING,
        actions: [
          {
            target: EXPECTED_RUN_RESULT.actions[0].target,
            status: dataform.ActionResult.ExecutionStatus.RUNNING,
            tasks: [EXPECTED_RUN_RESULT.actions[0].tasks[0]],
          },
        ],
      }).toJSON(),
    );

    runner = Runner.resume(mockDbAdapterInstance, RUN_TEST_GRAPH, result);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
    ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
  });

  suite("execute with retry", () => {
    test("should fail when execution fails too many times for the retry setting", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0),
          },
        },
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {},
      });
      when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()))
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { actionRetryLimit: 1 },
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    });

    test("should pass when execution fails initially, then passes with the number of allowed retries", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0),
          },
        },
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {},
      });
      when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()))
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { actionRetryLimit: 2 },
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
      ).to.deep.equal(
        dataform.RunResult.create({
          status: dataform.RunResult.ExecutionStatus.SUCCESSFUL,
          actions: [
            EXPECTED_RUN_RESULT.actions[0],
            {
              target: NEW_TEST_GRAPH.actions[1].target,
              tasks: [
                {
                  status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
                  metadata: {},
                },
              ],
              status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
            },
          ],
        }).toJSON(),
      );
    });

    test("should not retry when the task is an operation", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH_WITH_OPERATION = RUN_TEST_GRAPH;
      NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].type = "operation";

      when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0),
          },
        },
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
      ).thenResolve({
        rows: [],
        metadata: {},
      });
      when(
        mockedDbAdapter.execute(
          NEW_TEST_GRAPH_WITH_OPERATION.actions![1].tasks![0].statement!,
          anything(),
        ),
      )
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH_WITH_OPERATION, {
        bigquery: { actionRetryLimit: 3 },
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    });
  });

  test("execute_with_cancel", async () => {
    const CANCEL_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "foo",
        assertionSchema: "bar",
        defaultLocation: "US",
      },
      warehouseState: {
        tables: [],
      },
      actions: [
        {
          tasks: [
            {
              type: "statement",
              statement: "some statement",
            },
          ],
          type: "table",
          target: {
            schema: "schema1",
            name: "target1",
          },
          tableType: "table",
          dependencyTargets: [],
        },
      ],
    });

    let wasCancelled = false;
    const mockDbAdapter = {
      execute: (_, { onCancel }) =>
        new Promise((__, reject) => {
          onCancel(() => {
            wasCancelled = true;
            reject(new Error("Run cancelled"));
          });
        }),
      schemas: (_) => Promise.resolve([]),
      table: (_) => undefined,
    } as IDbAdapter;

    const runner = new Runner(mockDbAdapter, CANCEL_TEST_GRAPH);
    const execution = runner.execute().result();
    // We want to await the return promise before we actually call cancel.
    // Waiting a short (10ms) time before calling cancel accomplishes this.
    await sleep(10);
    runner.cancel();
    const result = await execution;
    expect(wasCancelled).equals(true);
    // Cancelling a run doesn't actually throw at the top level.
    // The action should fail, and have an appropriate error message.
    expect(result.actions[0].tasks[0].status).equal(dataform.TaskResult.ExecutionStatus.CANCELLED);
    expect(result.actions[0].tasks[0].errorMessage).to.match(/cancelled/);
  });

  suite("execute with bigquery labels", () => {
    test("should pass labels to executeTask", async () => {
      const executionOptions: Array<{ bigquery?: any }> = [];

      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0),
            },
          },
        });
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const labels = { env: "testing", team: "dataform" };
      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { labels },
      });

      const result = await runner.execute().result();
      expect(result.status).to.equal(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

      // Verify that execute was called at least 3 times (for both tasks in first action and assertion)
      expect(executionOptions.length).to.equal(3);

      // Verify that at least some calls included labels in the options
      const callsWithLabels = executionOptions.filter(
        (opts) =>
          opts?.bigquery?.labels &&
          opts.bigquery.labels.env === "testing" &&
          opts.bigquery.labels.team === "dataform",
      );
      expect(callsWithLabels.length).to.equal(
        3,
        "Expected 3 execute calls to include the labels in options",
      );
    });

    test("should merge global and action-level labels", async () => {
      const executionOptions: Array<{ bigquery?: any }> = [];

      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      // Set action-level labels on the first action
      NEW_TEST_GRAPH.actions![0].actionDescriptor = {
        bigqueryLabels: { action_level: "specific_value" },
      };

      when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0),
            },
          },
        });
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()),
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const globalLabels = { env: "testing", team: "dataform" };
      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { labels: globalLabels },
      });

      const result = await runner.execute().result();
      expect(result.status).to.equal(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

      // Verify that execute was called 3 times
      expect(executionOptions.length).to.equal(3);

      // For the first two calls (action with both task types), verify merged labels
      const firstActionCalls = executionOptions.slice(0, 2);
      firstActionCalls.forEach((opts, index) => {
        expect(opts?.bigquery?.labels).to.not.equal(undefined);
        // Should have global labels
        expect(opts.bigquery.labels.env).to.equal(
          "testing",
          `Call ${index} should have global label 'env'`,
        );
        expect(opts.bigquery.labels.team).to.equal(
          "dataform",
          `Call ${index} should have global label 'team'`,
        );
        // Should have action-level label
        expect(opts.bigquery.labels.action_level).to.equal(
          "specific_value",
          `Call ${index} should have action-level label 'action_level'`,
        );
      });

      // For the second action (assertion), verify only global labels (no action-level labels)
      const assertionCall = executionOptions[2];
      expect(assertionCall?.bigquery?.labels).to.not.equal(undefined);
      expect(assertionCall.bigquery.labels.env).to.equal("testing");
      expect(assertionCall.bigquery.labels.team).to.equal("dataform");
      // This action doesn't have action-level labels
      expect(assertionCall.bigquery.labels.action_level).to.equal(undefined);
    });
  });

  test("continues after setMetadata fails", async () => {
    const METADATA_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "foo",
        assertionSchema: "bar",
        defaultLocation: "US",
      },
      warehouseState: {
        tables: [],
      },
      actions: [
        {
          tasks: [
            {
              type: "statement",
              statement: "some statement",
            },
          ],
          type: "table",
          target: {
            schema: "schema1",
            name: "target1",
          },
          actionDescriptor: {
            description: "desc",
          },
          tableType: "table",
          dependencyTargets: [],
        },
      ],
    });
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1", "schema2"]);
    when(mockedDbAdapter.execute(anything(), anything())).thenResolve({
      rows: [],
      metadata: {},
    });
    when(mockedDbAdapter.setMetadata(anything())).thenReject(new Error("Error during setMetadata"));

    const mockDbAdapterInstance = instance(mockedDbAdapter);

    const runner = new Runner(mockDbAdapterInstance, METADATA_TEST_GRAPH);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON(),
    ).to.deep.equal({
      actions: [
        {
          status: "FAILED",
          target: {
            name: "target1",
            schema: "schema1",
          },
          tasks: [
            {
              errorMessage: "Error setting metadata: Error during setMetadata",
              metadata: {},
              status: "FAILED",
            },
          ],
        },
      ],
      status: "FAILED",
    });
  });

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
});

function cleanTiming(runResult: dataform.IRunResult) {
  const newRunResult = dataform.RunResult.create(runResult);
  delete newRunResult.timing;
  newRunResult.actions.forEach((actionResult) => {
    delete actionResult.timing;
    actionResult.tasks.forEach((taskResult) => {
      delete taskResult.timing;
    });
  });
  return newRunResult;
}
