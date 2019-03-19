import * as protos from "@dataform/protos";
import * as prettyMs from "pretty-ms";
import * as EventEmitter from "events";
import * as dbadapters from "../dbadapters";
import * as utils from "../utils";
import * as Long from "long";

const CANCEL_EVENT = "jobCancel";

export function run(graph: protos.IExecutionGraph, profile: protos.IProfile): Runner {
  utils.validateProfile(profile);
  const runner = Runner.create(dbadapters.create(profile, graph.projectConfig.warehouse), graph);
  runner.execute();
  return runner;
}

export class Runner {
  private adapter: dbadapters.DbAdapter;
  private graph: protos.IExecutionGraph;

  private pendingNodes: protos.IExecutionNode[];

  private cancelled = false;
  private result: protos.IExecutedGraph;

  private changeListeners: ((graph: protos.IExecutedGraph) => void)[] = [];

  private executionTask: Promise<protos.IExecutedGraph>;

  private eEmitter: EventEmitter;

  constructor(adapter: dbadapters.DbAdapter, graph: protos.IExecutionGraph) {
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

  public static create(adapter: dbadapters.DbAdapter, graph: protos.IExecutionGraph) {
    return new Runner(adapter, graph);
  }

  public onChange(listener: (graph: protos.IExecutedGraph) => void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public async execute(): Promise<protos.IExecutedGraph> {
    if (!!this.executionTask) throw Error("Executor already started.");
    const prepareDefaultSchema = this.adapter.prepareSchema(this.graph.projectConfig.defaultSchema);
    const prepareAssertionSchema = this.adapter.prepareSchema(this.graph.projectConfig.assertionSchema);

    this.executionTask = new Promise((resolve, reject) => {
      try {
        this.loop(() => resolve(this.result), reject);
      } catch (e) {
        reject(e);
      }
    });

    await prepareDefaultSchema;
    await prepareAssertionSchema;

    return this.executionTask;
  }

  public cancel() {
    this.cancelled = true;
    this.eEmitter.emit(CANCEL_EVENT);
  }

  public resultPromise(): Promise<protos.IExecutedGraph> {
    return this.executionTask;
  }

  private triggerChange() {
    this.changeListeners.forEach(listener => listener(this.result));
  }

  private loop(resolve: () => void, reject: (value: any) => void) {
    var pendingNodes = this.pendingNodes;
    this.pendingNodes = [];

    let allFinishedDeps = this.result.nodes.map(node => node.name);
    let allSuccessfulDeps = this.result.nodes
      .filter(
        node =>
          node.status === protos.NodeExecutionStatus.SUCCESSFUL || node.status == protos.NodeExecutionStatus.DISABLED
      )
      .map(fn => fn.name);

    pendingNodes.forEach(node => {
      let finishedDeps = node.dependencies.filter(d => allFinishedDeps.indexOf(d) >= 0);
      let successfulDeps = node.dependencies.filter(d => allSuccessfulDeps.indexOf(d) >= 0);
      if (!this.cancelled && successfulDeps.length == node.dependencies.length) {
        // All required deps are completed, start this node.
        this.executeNode(node);
      } else if (this.cancelled || finishedDeps.length == node.dependencies.length) {
        // All deps are finished but they weren't all successful, or the run was cancelled.
        // skip this node.
        console.log(`Completed node: "${node.name}", status: skipped`);
        this.result.nodes.push({
          name: node.name,
          status: protos.NodeExecutionStatus.SKIPPED,
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
      var ok = true;
      this.result.nodes.forEach(node => {
        ok =
          ok &&
          (node.status === protos.NodeExecutionStatus.SUCCESSFUL || node.status == protos.NodeExecutionStatus.DISABLED);
      });
      this.result.ok = ok;
      resolve();
    }
  }

  private executeNode(node: protos.IExecutionNode) {
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

            if (task.type == "assertion" && rows.length > 0) {
              throw [
                ...chainResults,
                {
                  ok: false,
                  task: task,
                  error: `Test failed: returned >= ${rows.length} rows.`
                }
              ];
            } else {
              return [...chainResults, { ok: true, task: task }];
            }
          } catch (e) {
            throw [...chainResults, { ok: false, error: e.message, task: task }];
          }
        });
      }, Promise.resolve([] as protos.IExecutedTask[]))
      .then((results: protos.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);
        const prettyTime = prettyMs(executionTime);

        console.log(`Completed node: "${node.name}", status: successful (${prettyTime})`);
        this.result.nodes.push({
          name: node.name,
          status: results.length == 0 ? protos.NodeExecutionStatus.DISABLED : protos.NodeExecutionStatus.SUCCESSFUL,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: true
        });
        this.triggerChange();
      })
      .catch((results: protos.IExecutedTask[]) => {
        const endTime = process.hrtime(startTime);
        const executionTime = endTime[0] * 1000 + Math.round(endTime[1] / 1000000);
        const prettyTime = prettyMs(executionTime);

        console.log(`Completed node: "${node.name}", status: failed (${prettyTime})`);
        this.result.nodes.push({
          name: node.name,
          status: protos.NodeExecutionStatus.FAILED,
          tasks: results,
          executionTime: Long.fromNumber(executionTime),
          deprecatedOk: false
        });
        this.triggerChange();
      });
  }
}
