import { expect } from "chai";
import { anything, instance, mock, when } from "ts-mockito";

import { Runner } from "df/cli/api";
import { IDbAdapter } from "df/cli/api/dbadapters";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { sleep } from "df/common/promises";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

const FAILING_STATEMENT = "failing statement";
const STRANDED_ACTION_ERROR_MESSAGE =
  "Action did not run: it is part of a dependency cycle, or depends on an action that is.";

function target(name: string): dataform.ITarget {
  return { schema: "schema1", name };
}

function operation(
  name: string,
  dependencies: string[],
  statement = "some statement",
): dataform.IExecutionAction {
  return {
    type: "operation",
    target: target(name),
    dependencyTargets: dependencies.map(target),
    tasks: [{ type: "statement", statement }],
  };
}

function graphOf(actions: dataform.IExecutionAction[]): dataform.IExecutionGraph {
  return {
    projectConfig: { warehouse: "bigquery", defaultSchema: "schema1", defaultLocation: "US" },
    runConfig: {},
    warehouseState: { tables: [] },
    actions,
  };
}

function dbAdapterFailingOn(statement: string): IDbAdapter {
  const mockedDbAdapter = mock(BigQueryDbAdapter);
  when(mockedDbAdapter.schemas(anything())).thenResolve(["schema1"]);
  when(mockedDbAdapter.execute(anything(), anything())).thenResolve({ rows: [], metadata: {} });
  when(mockedDbAdapter.execute(statement, anything())).thenReject(new Error("bad statement"));
  return instance(mockedDbAdapter);
}

async function runToJson(dbAdapter: IDbAdapter, actions: dataform.IExecutionAction[]) {
  const runResult = dataform.RunResult.create(
    await new Runner(dbAdapter, graphOf(actions)).execute().result(),
  );
  delete runResult.timing;
  runResult.actions.forEach((actionResult) => {
    delete actionResult.timing;
    actionResult.tasks.forEach((taskResult) => delete taskResult.timing);
  });
  return runResult.toJSON();
}

suite("@dataform/api/run skipped and stranded actions", () => {
  test("skips every transitive dependent of a failed action", async () => {
    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [
      operation("a", [], FAILING_STATEMENT),
      operation("b", ["a"]),
      operation("c", ["b"]),
      operation("d", ["c"]),
    ]);

    expect(result).to.deep.equal({
      status: "FAILED",
      actions: [
        {
          target: target("a"),
          status: "FAILED",
          tasks: [
            {
              status: "FAILED",
              compiledSql: FAILING_STATEMENT,
              errorMessage: "bigquery error: bad statement",
              metadata: {},
            },
          ],
        },
        { target: target("b"), status: "SKIPPED", tasks: [{ status: "SKIPPED" }] },
        { target: target("c"), status: "SKIPPED", tasks: [{ status: "SKIPPED" }] },
        { target: target("d"), status: "SKIPPED", tasks: [{ status: "SKIPPED" }] },
      ],
    });
  });

  test("reports one skipped result for each task of a skipped action", async () => {
    const twoTasks = operation("two_tasks", ["a"]);
    twoTasks.tasks.push({ type: "statement", statement: "another statement" });

    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [
      operation("a", [], FAILING_STATEMENT),
      twoTasks,
      operation("after_two_tasks", ["two_tasks"]),
    ]);

    expect(result.actions.slice(1)).to.deep.equal([
      {
        target: target("two_tasks"),
        status: "SKIPPED",
        tasks: [{ status: "SKIPPED" }, { status: "SKIPPED" }],
      },
      { target: target("after_two_tasks"), status: "SKIPPED", tasks: [{ status: "SKIPPED" }] },
    ]);
  });

  test("still runs actions that do not depend on the failed action", async () => {
    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [
      operation("a", [], FAILING_STATEMENT),
      operation("b", ["a"]),
      operation("c", ["b"]),
      operation("unrelated", []),
      operation("after_unrelated", ["unrelated"]),
    ]);

    const statusByName = Object.fromEntries(
      result.actions.map((actionResult: any) => [actionResult.target.name, actionResult.status]),
    );
    expect(statusByName).to.deep.equal({
      a: "FAILED",
      b: "SKIPPED",
      c: "SKIPPED",
      unrelated: "SUCCESSFUL",
      after_unrelated: "SUCCESSFUL",
    });
  });

  test("skips a dependent whose tasks are omitted or empty without throwing", async () => {
    const withoutTasks = operation("without_tasks", ["a"]);
    delete withoutTasks.tasks;
    const withEmptyTasks: dataform.IExecutionAction = {
      ...operation("with_empty_tasks", ["a"]),
      tasks: [],
    };

    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [
      operation("a", [], FAILING_STATEMENT),
      withoutTasks,
      withEmptyTasks,
    ]);

    expect(result).to.deep.equal({
      status: "FAILED",
      actions: [
        {
          target: target("a"),
          status: "FAILED",
          tasks: [
            {
              status: "FAILED",
              compiledSql: FAILING_STATEMENT,
              errorMessage: "bigquery error: bad statement",
              metadata: {},
            },
          ],
        },
        { target: target("without_tasks"), status: "SKIPPED" },
        { target: target("with_empty_tasks"), status: "SKIPPED" },
      ],
    });
  });

  test("treats an action with omitted tasks and no JiT code as disabled", async () => {
    const withoutTasks = operation("without_tasks", []);
    delete withoutTasks.tasks;

    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [withoutTasks]);

    expect(result).to.deep.equal({
      status: "SUCCESSFUL",
      actions: [{ target: target("without_tasks"), status: "DISABLED" }],
    });
  });

  test("fails the actions of a dependency cycle and the actions depending on it", async () => {
    const result = await runToJson(dbAdapterFailingOn(FAILING_STATEMENT), [
      operation("root", []),
      operation("a", ["root", "b"]),
      operation("b", ["a"]),
      operation("after_cycle", ["b"]),
    ]);

    const strandedTasks = [{ status: "FAILED", errorMessage: STRANDED_ACTION_ERROR_MESSAGE }];
    expect(result).to.deep.equal({
      status: "FAILED",
      actions: [
        {
          target: target("root"),
          status: "SUCCESSFUL",
          tasks: [{ status: "SUCCESSFUL", compiledSql: "some statement", metadata: {} }],
        },
        { target: target("a"), status: "FAILED", tasks: strandedTasks },
        { target: target("b"), status: "FAILED", tasks: strandedTasks },
        { target: target("after_cycle"), status: "FAILED", tasks: strandedTasks },
      ],
    });
  });

  test("a cancelled run reports pending actions as skipped with the cancellation reason", async () => {
    const withoutTasks = operation("without_tasks", ["a"]);
    delete withoutTasks.tasks;
    const twoTasks = operation("two_tasks", ["a"]);
    twoTasks.tasks.push({ type: "statement", statement: "another statement" });
    const dbAdapter = {
      execute: (_, { onCancel }) =>
        new Promise((__, reject) => onCancel(() => reject(new Error("Run cancelled")))),
      schemas: (_) => Promise.resolve(["schema1"]),
      table: (_) => undefined,
    } as IDbAdapter;

    const runner = new Runner(
      dbAdapter,
      graphOf([operation("a", []), operation("b", ["a"]), withoutTasks, twoTasks]),
    );
    const execution = runner.execute().result();
    await sleep(10);
    runner.cancel();
    const result = dataform.RunResult.create(await execution).toJSON();

    expect(result.status).to.equal("CANCELLED");
    expect(result.actions.slice(1)).to.deep.equal([
      {
        target: target("b"),
        status: "SKIPPED",
        tasks: [{ status: "SKIPPED", errorMessage: "Run cancelled" }],
      },
      {
        target: target("without_tasks"),
        status: "SKIPPED",
        tasks: [{ status: "SKIPPED", errorMessage: "Run cancelled" }],
      },
      {
        target: target("two_tasks"),
        status: "SKIPPED",
        tasks: [{ status: "SKIPPED", errorMessage: "Run cancelled" }, { status: "SKIPPED" }],
      },
    ]);
  });
});
