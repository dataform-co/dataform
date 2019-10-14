import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";
import * as EventEmitter from "events";
import * as Long from "long";

const CANCEL_EVENT = "jobCancel";

const isSuccessfulAction = (action: dataform.IExecutedAction) =>
  action.status === dataform.ActionExecutionStatus.SUCCESSFUL ||
  action.status == dataform.ActionExecutionStatus.DISABLED;

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
  private result: dataform.IExecutedGraph;

  private changeListeners: Array<(graph: dataform.IExecutedGraph) => void> = [];

  private executionTask: Promise<dataform.IExecutedGraph>;

  private eEmitter: EventEmitter;

  constructor(adapter: dbadapters.IDbAdapter, graph: dataform.IExecutionGraph) {
    this.adapter = adapter;
    this.graph = graph;
    this.pendingActions = graph.actions;
    this.result = {
      projectConfig: this.graph.projectConfig,
      runConfig: this.graph.runConfig,
      warehouseState: this.graph.warehouseState,
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

    this.executionTask = new Promise(async (resolve, reject) => {
      try {
        // Work out all the schemas we are going to need to create first.
        const uniqueSchemas: { [schema: string]: boolean } = {};
        this.graph.actions
          .filter(action => !!action.target)
          .map(action => action.target.schema)
          .filter(schema => !!schema)
          .forEach(schema => (uniqueSchemas[schema] = true));

        // Wait for all schemas to be created.
        await Promise.all(
          Object.keys(uniqueSchemas).map(schema => this.adapter.prepareSchema(schema))
        );

        await this.executeGraph();
        resolve(this.result);
      } catch (e) {
        reject(e);
      }
    });

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
    return Promise.all(this.changeListeners.map(listener => listener(this.result)));
  }

  private async executeGraph() {
    // Recursively execute all actions as they become executable.
    await this.executeAllActionsReadyForExecution();

    let ok = true;
    this.result.actions.forEach(action => {
      ok = ok && isSuccessfulAction(action);
    });
    this.result.ok = ok;
  }

  private async executeAllActionsReadyForExecution() {
    const allSuccessfulActions = this.result.actions.filter(isSuccessfulAction).map(fn => fn.name);
    const isReadyForExecution = (action: dataform.IExecutionAction) => {
      for (const dependency of action.dependencies) {
        if (!allSuccessfulActions.includes(dependency)) {
          return false;
        }
      }
      return true;
    };
    const readyForExecutionActions = this.pendingActions.filter(isReadyForExecution);
    this.pendingActions = this.pendingActions.filter(action => !isReadyForExecution(action));
    return Promise.all(
      readyForExecutionActions.map(async action => {
        this.result.actions.push(await this.executeAction(action));
        await this.triggerChange();
        await this.executeAllActionsReadyForExecution();
      })
    );
  }

  private async executeAction(action: dataform.IExecutionAction) {
    const startTime = new Date().valueOf();

    const executedTasks: dataform.IExecutedTask[] = [];
    let allSuccessful = true;
    for (const task of action.tasks) {
      if (allSuccessful) {
        const executedTask = await this.executeTask(task);
        executedTasks.push(executedTask);
        allSuccessful = allSuccessful && executedTask.ok;
      }
    }

    const endTime = new Date().valueOf();
    const executionTime = endTime - startTime;
    return {
      name: action.name,
      status: allSuccessful
        ? executedTasks.length == 0
          ? dataform.ActionExecutionStatus.DISABLED
          : dataform.ActionExecutionStatus.SUCCESSFUL
        : dataform.ActionExecutionStatus.FAILED,
      tasks: executedTasks,
      executionTime: Long.fromNumber(executionTime),
      deprecatedOk: allSuccessful
    };
  }

  private async executeTask(task: dataform.IExecutionTask): Promise<dataform.IExecutedTask> {
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
      return { ok: true, task };
    } catch (e) {
      return { ok: false, error: e.message, task };
    }
  }
}
