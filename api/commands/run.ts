import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";
import * as EventEmitter from "events";
import * as Long from "long";

const CANCEL_EVENT = "jobCancel";

export function run(graph: dataform.IExecutionGraph, credentials: Credentials): Runner {
  const runner = Runner.create(
    dbadapters.create(credentials, graph.projectConfig.warehouse),
    graph
  );
  runner.execute().catch(() => null);
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
  private result: dataform.IExecutionGraph;

  private changeListeners: Array<(graph: dataform.IExecutionGraph) => void> = [];

  private executionTask: Promise<dataform.IExecutionGraph>;

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

  public onChange(listener: (graph: dataform.IExecutionGraph) => Promise<void> | void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public async execute(): Promise<dataform.IExecutionGraph> {
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

        // Start the main execution loop.
        const _ = this.loop(() => resolve(this.result), reject).catch(reject);
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

  public resultPromise(): Promise<dataform.IExecutionGraph> {
    return this.executionTask;
  }

  private triggerChange() {
    const ignored = Promise.all(this.changeListeners.map(listener => listener(this.result)));
  }

  private async loop(resolve: () => void, reject: (value: any) => void) {
    const pendingActions = this.pendingActions;
    this.pendingActions = [];

    const allFinishedDeps = this.result.actions.map(action => action.name);
    const allSuccessfulDeps = this.result.actions
      .filter(
        action =>
          action.status === dataform.ActionExecutionStatus.Enum.SUCCESSFUL ||
          action.status === dataform.ActionExecutionStatus.Enum.DISABLED
      )
      .map(fn => fn.name);

    pendingActions.forEach(async action => {
      const finishedDeps = action.dependencies.filter(d => allFinishedDeps.indexOf(d) >= 0);
      const successfulDeps = action.dependencies.filter(d => allSuccessfulDeps.indexOf(d) >= 0);
      if (!this.cancelled && successfulDeps.length === action.dependencies.length) {
        // All required deps are completed, start this action.
        const ignored = this.executeAction(action).catch(e => reject(e));
      } else if (this.cancelled || finishedDeps.length === action.dependencies.length) {
        await this.triggerChange();
        // All deps are finished but they weren't all successful, or the run was cancelled.
        // skip this action.
        this.result.actions.push({
          name: action.name,
          status: dataform.ActionExecutionStatus.Enum.SKIPPED
        });
      } else {
        this.pendingActions.push(action);
      }
    });

    if (
      this.pendingActions.length > 0 ||
      this.result.actions.length !== this.graph.actions.length
    ) {
      setTimeout(() => this.loop(resolve, reject), 100);
    } else {
      // Work out if this run was an overall success.
      let ok = true;
      this.result.actions.forEach(action => {
        ok =
          ok &&
          (action.status === dataform.ActionExecutionStatus.Enum.SUCCESSFUL ||
            action.status === dataform.ActionExecutionStatus.Enum.DISABLED);
      });
      this.result.status = ok
        ? dataform.GraphExecutionStatus.Enum.SUCCESSFUL
        : dataform.GraphExecutionStatus.Enum.FAILED;
      resolve();
    }
  }

  private executeAction(action: dataform.IExecutionAction) {
    const actionStartTimeMillis = Date.now();
    // This creates a promise chain that executes all tasks in order.
    // If a task fails, we keep processing tasks but mark them ask skipped.
    let skipRemainingTasks = false;
    return action.tasks
      .reduce((chain, task) => {
        return chain.then(async chainResults => {
          const taskStartTimeMillis = Date.now();
          if (skipRemainingTasks) {
            return [
              ...chainResults,
              {
                ...task,
                status: dataform.TaskExecutionStatus.Enum.SKIPPED
                // Omit timing information as it doesn't make sense for skipped tasks.
              }
            ];
          }
          try {
            // Create another promise chain for retries, if we allow them.
            const rows = await this.adapter.execute(task.statement, {
              onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel)
            });
            if (task.type === "assertion") {
              // We expect that an assertion query returns 1 row, with 1 field that is the row count.
              // We don't really care what that field/column is called.
              const rowCount = rows[0][Object.keys(rows[0])[0]];
              if (rowCount > 0) {
                throw new Error(`Assertion failed: query returned ${rowCount} row(s).`);
              }
            }
            return [
              ...chainResults,
              {
                ...task,
                status: dataform.TaskExecutionStatus.Enum.SUCCESSFUL,
                timing: {
                  startTimeMillis: Long.fromNumber(taskStartTimeMillis),
                  endTimeMillis: Long.fromNumber(Date.now())
                }
              }
            ];
          } catch (e) {
            skipRemainingTasks = true;
            return [
              ...chainResults,
              {
                ...task,
                status: dataform.TaskExecutionStatus.Enum.FAILED,
                error: e.message,
                timing: {
                  startTimeMillis: Long.fromNumber(taskStartTimeMillis),
                  endTimeMillis: Long.fromNumber(Date.now())
                }
              }
            ];
          }
        });
      }, Promise.resolve([] as dataform.IExecutionTask[]))
      .then(async (results: dataform.IExecutionTask[]) => {
        const actionEndTimeMillis = Date.now();
        const actionSuccessful = !results.some(
          result => result.status === dataform.TaskExecutionStatus.Enum.FAILED
        );
        this.result.actions.push({
          ...action,
          status:
            results.length === 0
              ? dataform.ActionExecutionStatus.Enum.DISABLED
              : actionSuccessful
              ? dataform.ActionExecutionStatus.Enum.SUCCESSFUL
              : dataform.ActionExecutionStatus.Enum.FAILED,
          tasks: results,
          timing: {
            startTimeMillis: Long.fromNumber(actionStartTimeMillis),
            endTimeMillis: Long.fromNumber(actionEndTimeMillis)
          }
        });
        // We don't care if users listener code throws here.
        this.triggerChange();
      });
  }
}
