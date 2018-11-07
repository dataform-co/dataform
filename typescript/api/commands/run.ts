import * as protos from "@dataform/protos";
import * as dbadapters from "../dbadapters";

export function run(
  graph: protos.IExecutionGraph,
  profile: protos.IProfile
): Runner {
  var runner = Runner.create(
    dbadapters.create(profile, graph.projectConfig.warehouse),
    graph
  );
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

  constructor(adapter: dbadapters.DbAdapter, graph: protos.IExecutionGraph) {
    this.adapter = adapter;
    this.graph = graph;
    this.pendingNodes = graph.nodes;
    this.result = {
      projectConfig: this.graph.projectConfig,
      runConfig: this.graph.runConfig,
      nodes: []
    };
  }

  public static create(
    adapter: dbadapters.DbAdapter,
    graph: protos.IExecutionGraph
  ) {
    return new Runner(adapter, graph);
  }

  public onChange(listener: (graph: protos.IExecutedGraph) => void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public execute(): Promise<protos.IExecutedGraph> {
    if (!!this.executionTask) throw Error("Executor already started.");
    this.executionTask = new Promise((resolve, reject) => {
      try {
        this.adapter
          .prepareSchema(this.graph.projectConfig.defaultSchema)
          .then(() => this.loop(() => resolve(this.result), reject))
          .catch(e => reject(e));
      } catch (e) {
        reject(e);
      }
    });
    return this.executionTask;
  }

  public cancel() {
    this.cancelled = true;
  }

  public resultPromise(): Promise<protos.IExecutedGraph> {
    return this.executionTask;
  }

  private triggerChange() {
    this.changeListeners.forEach(listener => listener(this.result));
  }

  private loop(resolve: () => void, reject: (value: any) => void) {
    if (this.cancelled) {
      reject(Error("Run cancelled."));
    }
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
      setTimeout(() => this.loop(resolve, reject), 100);
    } else {
      resolve();
    }
  }

  private executeNode(node: protos.IExecutionNode) {
    // This creates a promise chain that executes all tasks in order.
    node.tasks
      .reduce((chain, task) => {
        return chain.then(chainResults => {
          // Create another promise chain for retries, if we allow them.
          return this.adapter
            .execute(task.statement)
            .then(rows => {
              if (task.type == "assertion" && rows.length > 0) {
                return [
                  ...chainResults,
                  {
                    ok: false,
                    task: task,
                    error: `Test failed, as it returned the following rows:\n${rows
                      .slice(0, Math.max(rows.length - 1, 10))
                      .map(row => JSON.stringify(row, null, 4))
                      .join("\n")}`
                  }
                ];
              }
              return [...chainResults, { ok: true, task: task }];
            })
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
