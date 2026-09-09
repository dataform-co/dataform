import { config, expect } from "chai";
import Long from "long";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

import { Runner } from "df/cli/api";
import { IDbAdapter } from "df/cli/api/dbadapters";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
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
      defaultLocation: "US"
    },
    runConfig: {
      fullRefresh: true
    },
    warehouseState: {
      tables: [
        {
          type: dataform.TableMetadata.Type.TABLE,
          target: {
            schema: "schema1",
            name: "target1"
          }
        }
      ]
    },
    actions: [
      {
        tasks: [
          {
            type: "executionTaskType",
            statement: "SELECT foo FROM bar"
          },
          {
            type: "executionTaskType",
            statement: "SELECT 42"
          }
        ],
        type: "table",
        target: {
          schema: "schema1",
          name: "target1"
        },
        tableType: "someTableType",
        dependencyTargets: []
      },
      {
        tasks: [
          {
            type: "executionTaskType2",
            statement: "SELECT bar FROM baz"
          }
        ],
        type: "assertion",
        target: {
          database: "database2",
          schema: "schema2",
          name: "target2"
        },
        tableType: "someTableType",
        dependencyTargets: [
          {
            schema: "schema1",
            name: "target1"
          }
        ]
      }
    ]
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
                totalBytesProcessed: Long.fromNumber(0)
              }
            }
          },
          {
            status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
            metadata: {}
          }
        ],
        status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
      },
      {
        target: RUN_TEST_GRAPH.actions![1].target,
        tasks: [
          {
            status: dataform.TaskResult.ExecutionStatus.FAILED,
            metadata: {},
            errorMessage: "bigquery error: bad statement"
          }
        ],
        status: dataform.ActionResult.ExecutionStatus.FAILED
      }
    ]
  });

  test("execute", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
    ).thenResolve({
      rows: [],
      metadata: {
        bigquery: {
          jobId: "abc",
          totalBytesBilled: Long.fromNumber(0),
          totalBytesProcessed: Long.fromNumber(0)
        }
      }
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
    ).thenResolve({
      rows: [],
      metadata: {}
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![1].tasks![0].statement!, anything())
    ).thenReject(new Error("bad statement"));

    const mockDbAdapterInstance = instance(mockedDbAdapter);

    const runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
    ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    verify(mockedDbAdapter.createSchema("database", "schema1")).once();
    verify(mockedDbAdapter.createSchema("database2", "schema2")).once();
  });

  test("stop and then resume", async () => {
    let firstQueryInProgress = false;
    let stopWasCalled = false;

    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
    ).thenCall(async () => {
      firstQueryInProgress = true;
      await sleepUntil(() => stopWasCalled);
      return {
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0)
          }
        }
      };
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
    ).thenResolve({
      rows: [],
      metadata: {}
    });
    when(
      mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![1].tasks![0].statement!, anything())
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
            tasks: [EXPECTED_RUN_RESULT.actions[0].tasks[0]]
          }
        ]
      }).toJSON()
    );

    runner = Runner.resume(mockDbAdapterInstance, RUN_TEST_GRAPH, result);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
    ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    verify(mockedDbAdapter.createSchema("database", "schema1")).once();
    verify(mockedDbAdapter.createSchema("database2", "schema2")).once();
  });

  suite("execute with retry", () => {
    test("should fail when execution fails too many times for the retry setting", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0)
          }
        }
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {}
      });
      when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()))
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { actionRetryLimit: 1 }
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    });

    test("should pass when execution fails initially, then passes with the number of allowed retries", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0)
          }
        }
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {}
      });
      when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything()))
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { actionRetryLimit: 2 }
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
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
                  metadata: {}
                }
              ],
              status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
            }
          ]
        }).toJSON()
      );
    });

    test("should not retry when the task is an operation", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH_WITH_OPERATION = RUN_TEST_GRAPH;
      NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].type = "operation";

      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0)
          }
        }
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
      ).thenResolve({
        rows: [],
        metadata: {}
      });
      when(
        mockedDbAdapter.execute(
          NEW_TEST_GRAPH_WITH_OPERATION.actions![1].tasks![0].statement!,
          anything()
        )
      )
        .thenReject(new Error("bad statement"))
        .thenReject(new Error("bad statement"))
        .thenResolve({ rows: [], metadata: {} });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH_WITH_OPERATION, {
        bigquery: { actionRetryLimit: 3 }
      });

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
    });
  });

  test("execute_with_cancel", async () => {
    const CANCEL_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "foo",
        assertionSchema: "bar",
        defaultLocation: "US"
      },
      warehouseState: {
        tables: []
      },
      actions: [
        {
          tasks: [
            {
              type: "statement",
              statement: "some statement"
            }
          ],
          type: "table",
          target: {
            schema: "schema1",
            name: "target1"
          },
          tableType: "table",
          dependencyTargets: []
        }
      ]
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
      schemas: _ => Promise.resolve([]),
      createSchema: (_, __) => Promise.resolve(),
      table: _ => undefined
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
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        });
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const labels = { env: "testing", team: "dataform" };
      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { labels }
      });

      const result = await runner.execute().result();
      expect(result.status).to.equal(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

      // Verify that execute was called at least 3 times (for both tasks in first action and assertion)
      expect(executionOptions.length).to.equal(3);

      // Verify that at least some calls included labels in the options
      const callsWithLabels = executionOptions.filter(
        opts =>
          opts?.bigquery?.labels &&
          opts.bigquery.labels.env === "testing" &&
          opts.bigquery.labels.team === "dataform"
      );
      expect(callsWithLabels.length).to.equal(
        3,
        "Expected 3 execute calls to include the labels in options"
      );
    });

    test("should merge global and action-level labels", async () => {
      const executionOptions: Array<{ bigquery?: any }> = [];

      const mockedDbAdapter = mock(BigQueryDbAdapter);
      const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
      // Set action-level labels on the first action
      NEW_TEST_GRAPH.actions![0].actionDescriptor = {
        bigqueryLabels: { action_level: "specific_value" }
      };

      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![0].tasks![0].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        });
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions![0].tasks![1].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });
      when(
        mockedDbAdapter.execute(NEW_TEST_GRAPH.actions![1].tasks![0].statement!, anything())
      ).thenCall((statement: string, options: any) => {
        executionOptions.push(options);
        return Promise.resolve({ rows: [], metadata: {} });
      });

      const mockDbAdapterInstance = instance(mockedDbAdapter);

      const globalLabels = { env: "testing", team: "dataform" };
      const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
        bigquery: { labels: globalLabels }
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
          `Call ${index} should have global label 'env'`
        );
        expect(opts.bigquery.labels.team).to.equal(
          "dataform",
          `Call ${index} should have global label 'team'`
        );
        // Should have action-level label
        expect(opts.bigquery.labels.action_level).to.equal(
          "specific_value",
          `Call ${index} should have action-level label 'action_level'`
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
        defaultLocation: "US"
      },
      warehouseState: {
        tables: []
      },
      actions: [
        {
          tasks: [
            {
              type: "statement",
              statement: "some statement"
            }
          ],
          type: "table",
          target: {
            schema: "schema1",
            name: "target1"
          },
          actionDescriptor: {
            description: "desc"
          },
          tableType: "table",
          dependencyTargets: []
        }
      ]
    });
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
    when(mockedDbAdapter.execute(anything(), anything())).thenResolve({
      rows: [],
      metadata: {}
    });
    when(mockedDbAdapter.setMetadata(anything())).thenReject(new Error("Error during setMetadata"));

    const mockDbAdapterInstance = instance(mockedDbAdapter);

    const runner = new Runner(mockDbAdapterInstance, METADATA_TEST_GRAPH);

    expect(
      dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
    ).to.deep.equal({
      actions: [
        {
          status: "FAILED",
          target: {
            name: "target1",
            schema: "schema1"
          },
          tasks: [
            {
              errorMessage: "Error setting metadata: Error during setMetadata",
              metadata: {},
              status: "FAILED"
            }
          ]
        }
      ],
      status: "FAILED"
    });
  });
});

function cleanTiming(runResult: dataform.IRunResult) {
  const newRunResult = dataform.RunResult.create(runResult);
  delete newRunResult.timing;
  newRunResult.actions.forEach(actionResult => {
    delete actionResult.timing;
    actionResult.tasks.forEach(taskResult => {
      delete taskResult.timing;
    });
  });
  return newRunResult;
}
