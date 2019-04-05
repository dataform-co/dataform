import * as dbadapters from "@dataform/api/dbadapters";
import * as utils from "@dataform/api/utils";
import { dataform } from "@dataform/protos";
import * as EventEmitter from "events";
import * as Long from "long";
import * as prettyMs from "pretty-ms";

const CANCEL_EVENT = "jobCancel";

export function run(graph: dataform.IExecutionGraph, credentials: utils.Credentials): Runner {
  const runner = Runner.create(
    dbadapters.create(credentials, graph.projectConfig.warehouse),
    graph
  );
  runner.execute();
  return runner;
}

export class Runner {
  public static create(adapter: dbadapters.DbAdapter, graph: dataform.IExecutionGraph) {
    return new Runner(adapter, graph);
  }
  private adapter: dbadapters.DbAdapter;
  private graph: dataform.IExecutionGraph;

  private pendingNodes: dataform.IExecutionNode[];

  private cancelled = false;
  private result: dataform.IExecutedGraph;

  private changeListeners: Array<(graph: dataform.IExecutedGraph) => void> = [];

  private executionTask: Promise<dataform.IExecutedGraph>;

  private eEmitter: EventEmitter;

  constructor(adapter: dbadapters.DbAdapter, graph: dataform.IExecutionGraph) {
    this.adapter = adapter;
    this.graph = graph;
    this.pendingNodes = graph.nodes;
    this.result = {
      projectConfig: this.graph.projectConfig,
      runConfig: this.graph.runConfig,
      warehouseState: this.graph.warehouseState,
      nodes: []
    };
    this.eEmitter = new EventEmitter();
    // There could feasibly be thousands of listeners to this, 0 makes the limit infinite.
    this.eEmitter.setMaxListeners(0);
  }

  public onChange(listener: (graph: dataform.IExecutedGraph) => void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public async execute(): Promise<dataform.IExecutedGraph> {
    if (!!this.executionTask) {
      throw Error("Executor already started.");
    }
    const prepareDefaultSchema = this.adapter.prepareSchema(this.graph.projectConfig.defaultSchema);
    const prepareAssertionSchema = this.adapter.prepareSchema(
      this.graph.projectConfig.assertionSchema
    );

    this.executionTask = new Promise(async (resolve, reject) => {
      try {
        await prepareDefaultSchema;
        await prepareAssertionSchema;
        this.loop(() => resolve(this.result), reject);
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
    this.changeListeners.forEach(listener => listener(this.result));
  }

  private loop(resolve: () => void, reject: (value: any) => void) {
    const pendingNodes = this.pendingNodes;
    this.pendingNodes = [];

    const allFinishedDeps = this.result.nodes.map(node => node.name);
    const allSuccessfulDeps = this.result.nodes
      .filter(
        node =>
          node.status === dataform.NodeExecutionStatus.SUCCESSFUL ||
          node.status == dataform.NodeExecutionStatus.DISABLED
      )
      .map(fn => fn.name);

    pendingNodes.forEach(node => {
      const finishedDeps = node.dependencies.filter(d => allFinishedDeps.indexOf(d) >= 0);
      const successfulDeps = node.dependencies.filter(d => allSuccessfulDeps.indexOf(d) >= 0);
      if (!this.cancelled && successfulDeps.length == node.dependencies.length) {
        // All required deps are completed, start this node.
        this.executeNode(node);
      } else if (this.cancelled || finishedDeps.length == node.dependencies.length) {
        // All deps are finished but they weren't all successful, or the run was cancelled.
        // skip this node.
        this.result.nodes.push({
          name: node.name,
          status: dataform.NodeExecutionStatus.SKIPPED,
          deprecatedSkipped: true
        });
        this.triggerChange();
      } else {
        this.pendingNodes.push(node);
        this.triggerChange();
      }
    });
    if (this.pendingNodes.length > 0 || this.result.nodes.length != this.graph.nodes.length) {
      setTimeout(() => this.loop(resolve, reject), 100);
    } else {
      // Work out if this run was an overall success.
      let ok = true;
      this.result.nodes.forEach(node => {
        ok =
          ok &&
          (node.status === dataform.NodeExecutionStatus.SUCCESSFUL ||
            node.status == dataform.NodeExecutionStatus.DISABLED);
      });
      this.result.ok = ok;
      resolve();
    }
  }

  private executeNode(node: dataform.IExecutionNode) {
    const startTime = process.hrtime();
    // This creates a promise chain that executes all tasks in order.
    node.tasks
      .reduce((chain, task) => {
        return chain.then(async chainResults => {
          try {
            // Create another promise chain for retries, if we allow them.
            const rows = await this.adapter.execute(task.statement, handleCancel =>
              this.eEmitter.on(CANCEL_EVENT, handleCancel)
            );
            if (task.type === "assertion") {
              // We expect that an assertion query returns 1 row, with 1 field that is the row count.
              // We don't really care what that field/column is called.
              const rowCount = rows[0][Object.keys(rows[0])[0]];
              if (rowCount > 0) {
                throw [
                  ...chainResults,
                  {
                    ok: false,
                    task,
                    error: `Test failed: query returned ${rowCount} row(s).`
                  }
                ];
              }
            }
            return [...chainResults, { ok: true, task }];
          } catch (e) {
            throw [...chainResults, { ok: false, error: e.message, task }];
          }
        });
      }, Promise.resolve([] as dataform.IExecutedTask[]))
      .then((results: dataform.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);

        this.result.nodes.push({
          name: node.name,
          status:
            results.length == 0
              ? dataform.NodeExecutionStatus.DISABLED
              : dataform.NodeExecutionStatus.SUCCESSFUL,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: true
        });
        this.triggerChange();
      })
      .catch((results: dataform.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);

        this.result.nodes.push({
          name: node.name,
          status: dataform.NodeExecutionStatus.FAILED,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: false
        });
        this.triggerChange();
      });
  }
}
