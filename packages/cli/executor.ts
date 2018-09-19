import * as runners from "./runners";
import { protos } from "@dataform/core";

export class Executor {
  private runner: runners.Runner;
  private graph: protos.IExecutionGraph;

  private pendingNodes: protos.IExecutionNode[];

  private finishedNodes: protos.IExecutedNode[] = [];

  constructor(runner: runners.Runner, graph: protos.IExecutionGraph) {
    this.runner = runner;
    this.graph = graph;
    this.pendingNodes = graph.nodes;
  }

  public static execute(runner: runners.Runner, graph: protos.IExecutionGraph) {
    return new Executor(runner, graph).execute();
  }

  private execute(): Promise<protos.IExecutedGraph> {
    return new Promise((resolve, reject) => {
      try {
        this.loop(() =>
          resolve({
            projectConfig: this.graph.projectConfig,
            runConfig: this.graph.runConfig,
            nodes: this.finishedNodes
          })
        );
      } catch (e) {
        reject(e);
      }
    });
  }

  private loop(resolve: () => void) {
    var pendingNodes = this.pendingNodes;
    this.pendingNodes = [];

    let allFinishedDeps = this.finishedNodes.map(fn => fn.name);
    let allSuccessfulDeps = this.finishedNodes
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
        this.finishedNodes.push({ name: node.name, skipped: true });
      } else {
        this.pendingNodes.push(node);
      }
    });
    if (
      this.pendingNodes.length > 0 ||
      this.finishedNodes.length != this.graph.nodes.length
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
        this.finishedNodes.push({ name: node.name, ok: true, tasks: results });
      })
      .catch((results: protos.IExecutedTask[]) => {
        this.finishedNodes.push({ name: node.name, ok: false, tasks: results });
      });
  }
}
