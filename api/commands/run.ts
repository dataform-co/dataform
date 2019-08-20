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
      throw Error("Executor already started.");
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

  public resultPromise(): Promise<dataform.IExecutedGraph> {
    return this.executionTask;
  }

  private triggerChange() {
    return Promise.all(this.changeListeners.map(listener => listener(this.result)));
  }

  private async loop(resolve: () => void, reject: (value: any) => void) {
    const pendingActions = this.pendingActions;
    this.pendingActions = [];

    const allFinishedDeps = this.result.actions.map(action => action.name);
    const allSuccessfulDeps = this.result.actions
      .filter(
        action =>
          action.status === dataform.ActionExecutionStatus.SUCCESSFUL ||
          action.status == dataform.ActionExecutionStatus.DISABLED
      )
      .map(fn => fn.name);

    pendingActions.forEach(async action => {
      const finishedDeps = action.dependencies.filter(d => allFinishedDeps.indexOf(d) >= 0);
      const successfulDeps = action.dependencies.filter(d => allSuccessfulDeps.indexOf(d) >= 0);
      if (!this.cancelled && successfulDeps.length == action.dependencies.length) {
        // All required deps are completed, start this action.
        this.executeAction(action);
      } else if (this.cancelled || finishedDeps.length == action.dependencies.length) {
        await this.triggerChange();
        // All deps are finished but they weren't all successful, or the run was cancelled.
        // skip this action.
        this.result.actions.push({
          name: action.name,
          status: dataform.ActionExecutionStatus.SKIPPED,
          deprecatedSkipped: true
        });
      } else {
        this.pendingActions.push(action);
      }
    });

    if (this.pendingActions.length > 0 || this.result.actions.length != this.graph.actions.length) {
      setTimeout(() => this.loop(resolve, reject), 100);
    } else {
      // Work out if this run was an overall success.
      let ok = true;
      this.result.actions.forEach(action => {
        ok =
          ok &&
          (action.status === dataform.ActionExecutionStatus.SUCCESSFUL ||
            action.status == dataform.ActionExecutionStatus.DISABLED);
      });
      this.result.ok = ok;
      resolve();
    }
  }

  private executeAction(action: dataform.IExecutionAction) {
    const startTime = process.hrtime();
    // This creates a promise chain that executes all tasks in order.
    action.tasks
      .reduce((chain, task) => {
        return chain.then(async chainResults => {
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
            return [...chainResults, { ok: true, task }];
          } catch (e) {
            throw [...chainResults, { ok: false, error: e.message, task }];
          }
        });
      }, Promise.resolve([] as dataform.IExecutedTask[]))
      .then(async (results: dataform.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);
        await this.triggerChange();
        this.result.actions.push({
          name: action.name,
          status:
            results.length == 0
              ? dataform.ActionExecutionStatus.DISABLED
              : dataform.ActionExecutionStatus.SUCCESSFUL,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: true
        });
      })
      .catch(async (results: dataform.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);
        await this.triggerChange();
        this.result.actions.push({
          name: action.name,
          status: dataform.ActionExecutionStatus.FAILED,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: false
        });
      });
  }
}
