import { utils, adapters } from "@dataform/core";
import * as protos from "@dataform/protos";
import * as dbadapters from "../dbadapters";
import { state } from "../commands/state";

export function build(compiledGraph: protos.ICompiledGraph, runConfig: protos.IRunConfig, profile: protos.IProfile) {
  return state(compiledGraph, dbadapters.create(profile)).then(state => {
    return new Builder(compiledGraph, runConfig, state).build();
  });
}

export class Builder {
  private compiledGraph: protos.ICompiledGraph;
  private runConfig: protos.IRunConfig;

  private adapter: adapters.IAdapter;
  private state: protos.IWarehouseState;

  constructor(compiledGraph: protos.ICompiledGraph, runConfig: protos.IRunConfig, state: protos.IWarehouseState) {
    this.compiledGraph = compiledGraph;
    this.runConfig = runConfig;
    this.state = state;
    this.adapter = adapters.create(compiledGraph.projectConfig);
  }

  build(): protos.ExecutionGraph {
    var tableStateByTarget: { [targetJson: string]: protos.ITableState } = {};
    this.state.tables.forEach(tableState => {
      tableStateByTarget[JSON.stringify(tableState.target)] = tableState;
    });

    // Firstly, turn every thing into an execution node.
    var allNodes: protos.IExecutionNode[] = [].concat(
      this.compiledGraph.tables.map(t => this.buildTable(t, tableStateByTarget[JSON.stringify(t.target)])),
      this.compiledGraph.operations.map(o => this.buildOperation(o)),
      this.compiledGraph.assertions.map(a => this.buildAssertion(a))
    );
    var allNodeNames = allNodes.map(n => n.name);
    var nodeNameMap: { [name: string]: protos.IExecutionNode } = {};
    allNodes.forEach(node => (nodeNameMap[node.name] = node));

    // Determine which nodes should be included.
    var includedNodeNames =
      this.runConfig.nodes && this.runConfig.nodes.length > 0
        ? utils.matchPatterns(this.runConfig.nodes, allNodeNames)
        : allNodeNames;
    var includedNodes = allNodes.filter(node => includedNodeNames.indexOf(node.name) >= 0);
    if (this.runConfig.includeDependencies) {
      // Compute all transitive dependencies.
      for (let i = 0; i < allNodes.length; i++) {
        includedNodes.forEach(node => {
          var matchingNodeNames =
            node.dependencies && node.dependencies.length > 0
              ? utils.matchPatterns(node.dependencies, allNodeNames)
              : [];
          // Update included node names.
          matchingNodeNames.forEach(nodeName => {
            if (includedNodeNames.indexOf(nodeName) < 0) {
              includedNodeNames.push(nodeName);
            }
          });
          // Update included nodes.
          includedNodes = allNodes.filter(node => includedNodeNames.indexOf(node.name) >= 0);
        });
      }
    }
    // Remove any excluded dependencies.
    includedNodes.forEach(node => {
      node.dependencies = node.dependencies.filter(dep => includedNodeNames.indexOf(dep) >= 0);
    });
    return protos.ExecutionGraph.create({
      projectConfig: this.compiledGraph.projectConfig,
      runConfig: this.runConfig,
      warehouseState: this.state,
      nodes: includedNodes
    });
  }

  buildTable(t: protos.ITable, tableMetadata: protos.ITableMetadata) {
    const emptyTasks = [] as protos.IExecutionTask[];

    const tasks = t.disabled
      ? emptyTasks
      : emptyTasks.concat(
          (t.preOps || []).map(pre => ({ statement: pre })),
          this.adapter.publishTasks(t, this.runConfig, tableMetadata).build(),
          (t.postOps || []).map(post => ({ statement: post }))
        );

    return protos.ExecutionNode.create({
      name: t.name,
      dependencies: t.dependencies,
      tasks
    });
  }

  buildOperation(operation: protos.IOperation) {
    return protos.ExecutionNode.create({
      name: operation.name,
      dependencies: operation.dependencies,
      tasks: operation.queries.map(statement => ({
        type: "statement",
        statement: statement
      }))
    });
  }

  buildAssertion(assertion: protos.IAssertion) {
    return protos.ExecutionNode.create({
      name: assertion.name,
      dependencies: assertion.dependencies,
      tasks: this.adapter.assertTasks(assertion, this.compiledGraph.projectConfig).build()
    });
  }
}
