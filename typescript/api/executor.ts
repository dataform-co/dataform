import * as runners from "./runners";
import * as protos from "@dataform/protos";

export class Executor {
  private runner: runners.Runner;
  private graph: protos.IExecutionGraph;

  private pendingNodes: protos.IExecutionNode[];

  private result: protos.IExecutedGraph;
  private running: boolean = false;

  private changeListeners: ((graph: protos.IExecutedGraph) => void)[] = [];

  constructor(runner: runners.Runner, graph: protos.IExecutionGraph) {
    this.runner = runner;
    this.graph = graph;
    this.pendingNodes = graph.nodes;
    this.result = {
      projectConfig: this.graph.projectConfig,
      runConfig: this.graph.runConfig,
      nodes: []
    };
  }

  public static create(runner: runners.Runner, graph: protos.IExecutionGraph) {
    return new Executor(runner, graph);
  }

  public onChange(listener: (graph: protos.IExecutedGraph) => void) {
    this.changeListeners.push(listener);
    return this;
  }

  public execute(): Promise<protos.IExecutedGraph> {
    if (this.running) throw Error("Executor already started.");

    return new Promise((resolve, reject) => {
      try {
        this.loop(() => resolve(this.result));
      } catch (e) {
        reject(e);
      }
    });
  }

  private triggerChange() {
    this.changeListeners.forEach(listener => listener(this.result));
  }

  private loop(resolve: () => void) {
    var pendingNodes = this.pendingNodes;
    this.pendingNodes = [];

    let allFinishedDeps = this.result.nodes.map(fn => fn.name);
    let allSuccessfulDeps = this.result.nodes
      .filter(fn => fn.ok)
      .map(fn => fn.name);

    pendingNodes.forEach(node => {
      let finishedDeps = node.dependencies.filter(
        d => allFinishedDeps.indexOf(d) >= 0
      );
      let successfulDeps = node.dependencies.filter(
        d => allSuccessfulDeps.indexOf(d) >= 0
      );
      if (successfulDeps.length == node.dependencies.length) {
        // All required deps are completed, start this node.
        this.executeNode(node);
      } else if (finishedDeps.length == node.dependencies.length) {
        // All deps are finished but they weren't all successful, skip this node.
        this.result.nodes.push({ name: node.name, skipped: true });
        this.triggerChange();
      } else {
        this.pendingNodes.push(node);
        this.triggerChange();
      }
    });
    if (
      this.pendingNodes.length > 0 ||
      this.result.nodes.length != this.graph.nodes.length
    ) {
      setTimeout(() => this.loop(resolve), 100);
    } else {
      resolve();
    }
  }

  private executeNode(node: protos.IExecutionNode) {
    // This creates a promise chain that executes all tasks in order.
    var executedTasks = node.tasks
      .reduce((chain, task) => {
        return chain.then(chainResults => {
          // Create another promise chain for retries, if we allow them.
          return this.runner
            .execute(task.statement)
            .then(rows => [...chainResults, { ok: true, task: task }])
            .catch(e => {
              var newChainResults = [
                ...chainResults,
                { ok: !!task.ignoreErrors, error: e.message, task: task }
              ];
              if (task.ignoreErrors || this.graph.runConfig.carryOn) {
                return newChainResults;
                // If we can ignore erros on this task, continue.
              } else {
                // This task is not allowed to fail, kill the promise chain.
                throw newChainResults;
              }
            });
        });
      }, Promise.resolve([] as protos.IExecutedTask[]))
      .then(results => {
        this.result.nodes.push({ name: node.name, ok: true, tasks: results });
        this.triggerChange();
      })
      .catch((results: protos.IExecutedTask[]) => {
        this.result.nodes.push({ name: node.name, ok: false, tasks: results });
        this.triggerChange();
      });
  }
}
