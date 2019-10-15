import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";
import * as EventEmitter from "events";
import * as Long from "long";

const CANCEL_EVENT = "jobCancel";

const isSuccessfulAction = (actionResult: dataform.IActionResult) =>
  actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.DISABLED;

const actionExecutionStatusMap = new Map<
  dataform.ActionResult.ExecutionStatus,
  dataform.ActionExecutionStatus
>();
actionExecutionStatusMap.set(
  dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
  dataform.ActionExecutionStatus.SUCCESSFUL
);
actionExecutionStatusMap.set(
  dataform.ActionResult.ExecutionStatus.FAILED,
  dataform.ActionExecutionStatus.FAILED
);
actionExecutionStatusMap.set(
  dataform.ActionResult.ExecutionStatus.SKIPPED,
  dataform.ActionExecutionStatus.SKIPPED
);
actionExecutionStatusMap.set(
  dataform.ActionResult.ExecutionStatus.DISABLED,
  dataform.ActionExecutionStatus.DISABLED
);

export function run(graph: dataform.IExecutionGraph, credentials: Credentials): Runner {
  const runner = Runner.create(
    dbadapters.create(credentials, graph.projectConfig.warehouse),
    graph
  );
  runner.execute();
  return runner;
}

export class Runner {
  public static create(adapter: dbadapters.IDbAdapter, graph: dataform.IExecutionGraph) {
    return new Runner(adapter, graph);
  }
  private adapter: dbadapters.IDbAdapter;
  private graph: dataform.IExecutionGraph;

  private pendingActions: dataform.IExecutionAction[];

  private cancelled = false;
  private runResult: dataform.IRunResult;

  private changeListeners: Array<(graph: dataform.IExecutedGraph) => void> = [];

  private executionTask: Promise<dataform.IExecutedGraph>;

  private eEmitter: EventEmitter;

  constructor(adapter: dbadapters.IDbAdapter, graph: dataform.IExecutionGraph) {
    this.adapter = adapter;
    this.graph = graph;
    this.pendingActions = graph.actions;
    this.runResult = {
      actions: []
    };
    this.eEmitter = new EventEmitter();
    // There could feasibly be thousands of listeners to this, 0 makes the limit infinite.
    this.eEmitter.setMaxListeners(0);
  }

  public onChange(listener: (graph: dataform.IExecutedGraph) => Promise<void> | void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public async execute(): Promise<dataform.IExecutedGraph> {
    if (!!this.executionTask) {
      throw new Error("Executor already started.");
    }
    this.executionTask = this.executeGraph();
    return this.executionTask;
  }

  public cancel() {
    this.cancelled = true;
    this.eEmitter.emit(CANCEL_EVENT);
  }

  public resultPromise(): Promise<dataform.IExecutedGraph> {
    return this.executionTask;
  }

  private triggerChange() {
    const executedGraph = this.resultAsExecutedGraph();
    return Promise.all(this.changeListeners.map(listener => listener(executedGraph)));
  }

  private resultAsExecutedGraph() {
    const executedGraph: dataform.IExecutedGraph = {
      projectConfig: this.graph.projectConfig,
      runConfig: this.graph.runConfig,
      warehouseState: this.graph.warehouseState,
      actions: this.runResult.actions.map(actionResult => ({
        name: actionResult.name,
        status: actionExecutionStatusMap.get(actionResult.status),
        tasks: actionResult.tasks.map((taskResult, i) => {
          const executedTask: dataform.IExecutedTask = {
            ok: taskResult.status === dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
            task: this.graph.actions.find(action => action.name === actionResult.name).tasks[i]
          };
          if (!!taskResult.errorMessage) {
            executedTask.error = taskResult.errorMessage;
          }
          return executedTask;
        }),
        executionTime: actionResult.timing.endTimeMillis.subtract(
          actionResult.timing.startTimeMillis
        ),
        deprecatedOk: isSuccessfulAction(actionResult)
      }))
    };
    if (!!this.runResult.status) {
      executedGraph.ok = this.runResult.status === dataform.RunResult.ExecutionStatus.SUCCESSFUL;
    }
    return executedGraph;
  }

  private async executeGraph() {
    const timer = Timer.start();

    // Work out all the schemas we are going to need to create first.
    const uniqueSchemas: { [schema: string]: boolean } = {};
    this.graph.actions
      .filter(action => !!action.target)
      .map(action => action.target.schema)
      .filter(schema => !!schema)
      .forEach(schema => (uniqueSchemas[schema] = true));

    // Wait for all schemas to be created.
    await Promise.all(Object.keys(uniqueSchemas).map(schema => this.adapter.prepareSchema(schema)));

    // Recursively execute all actions as they become executable.
    await this.executeAllActionsReadyForExecution();

    this.runResult.timing = timer.end();

    let allSuccessful = true;
    this.runResult.actions.forEach(action => {
      allSuccessful = allSuccessful && isSuccessfulAction(action);
    });
    this.runResult.status = allSuccessful
      ? dataform.RunResult.ExecutionStatus.SUCCESSFUL
      : dataform.RunResult.ExecutionStatus.FAILED;

    return this.resultAsExecutedGraph();
  }

  private async executeAllActionsReadyForExecution() {
    // If the run has been cancelled, skip all pending actions.
    if (this.cancelled) {
      const allPendingActions = this.pendingActions;
      this.pendingActions = [];
      await Promise.all(
        allPendingActions.map(async pendingAction =>
          this.onActionExecutionComplete(toSkippedAction(pendingAction))
        )
      );
      return;
    }

    // Determine what actions we can execute.
    const allDependenciesHaveExecutedSuccessfully = allDependenciesHaveBeenExecuted(
      this.runResult.actions.filter(isSuccessfulAction)
    );
    const executableActions = this.pendingActions.filter(allDependenciesHaveExecutedSuccessfully);
    this.pendingActions = this.pendingActions.filter(
      action => !allDependenciesHaveExecutedSuccessfully(action)
    );

    // Determine what actions we should skip (necessarily excluding executable actions).
    const allDependenciesHaveExecuted = allDependenciesHaveBeenExecuted(this.runResult.actions);
    const skippableActions = this.pendingActions.filter(allDependenciesHaveExecuted);
    this.pendingActions = this.pendingActions.filter(
      action => !allDependenciesHaveExecuted(action)
    );

    await Promise.all([
      Promise.all(
        skippableActions.map(async skippableAction =>
          this.onActionExecutionComplete(toSkippedAction(skippableAction))
        )
      ),
      Promise.all(
        executableActions.map(async executableAction =>
          this.onActionExecutionComplete(await this.executeAction(executableAction))
        )
      )
    ]);
  }

  private async onActionExecutionComplete(actionResult: dataform.IActionResult) {
    this.runResult.actions.push(actionResult);
    await this.triggerChange();
    await this.executeAllActionsReadyForExecution();
  }

  private async executeAction(action: dataform.IExecutionAction): Promise<dataform.IActionResult> {
    const timer = Timer.start();

    const executedTasks: dataform.ITaskResult[] = [];
    let allSuccessful = true;
    for (const task of action.tasks) {
      if (allSuccessful) {
        const executedTask = await this.executeTask(task);
        executedTasks.push(executedTask);
        allSuccessful =
          allSuccessful && executedTask.status === dataform.TaskResult.ExecutionStatus.SUCCESSFUL;
      }
    }

    return {
      name: action.name,
      status: allSuccessful
        ? executedTasks.length === 0
          ? dataform.ActionResult.ExecutionStatus.DISABLED
          : dataform.ActionResult.ExecutionStatus.SUCCESSFUL
        : dataform.ActionResult.ExecutionStatus.FAILED,
      tasks: executedTasks,
      timing: timer.end()
    };
  }

  private async executeTask(task: dataform.IExecutionTask): Promise<dataform.ITaskResult> {
    const timer = Timer.start();
    try {
      const rows = await this.adapter.execute(task.statement, {
        onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel),
        maxResults: 1
      });
      if (task.type === "assertion") {
        // We expect that an assertion query returns 1 row, with 1 field that is the row count.
        // We don't really care what that field/column is called.
        const rowCount = rows[0][Object.keys(rows[0])[0]];
        if (rowCount > 0) {
          throw new Error(`Assertion failed: query returned ${rowCount} row(s).`);
        }
      }
      return {
        status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
        timing: timer.end()
      };
    } catch (e) {
      return {
        status: dataform.TaskResult.ExecutionStatus.FAILED,
        errorMessage: e.message,
        timing: timer.end()
      };
    }
  }
}

function allDependenciesHaveBeenExecuted(actionResults: dataform.IActionResult[]) {
  const executedActionNames = actionResults.map(action => action.name);
  return (action: dataform.IExecutionAction) => {
    for (const dependency of action.dependencies) {
      if (!executedActionNames.includes(dependency)) {
        return false;
      }
    }
    return true;
  };
}

function toSkippedAction(action: dataform.IExecutionAction): dataform.IActionResult {
  return {
    name: action.name,
    status: dataform.ActionResult.ExecutionStatus.SKIPPED
  };
}

class Timer {
  private constructor(readonly startTimeMillis: number) {}
  public static start() {
    return new Timer(new Date().valueOf());
  }

  public end(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
      endTimeMillis: Long.fromNumber(new Date().valueOf())
    };
  }
}
