import { adapters, utils } from "@dataform/core";
import { dataform } from "@dataform/protos";
import { state } from "@dataform/api/commands/state";
import * as dbadapters from "@dataform/api/dbadapters";
import * as util from "@dataform/api/utils";

export function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  credentials: util.Credentials
) {
  return state(compiledGraph, dbadapters.create(credentials)).then(state => {
    return new Builder(compiledGraph, runConfig, state).build();
  });
}

export class Builder {
  private compiledGraph: dataform.ICompiledGraph;
  private runConfig: dataform.IRunConfig;

  private adapter: adapters.IAdapter;
  private state: dataform.IWarehouseState;

  constructor(
    compiledGraph: dataform.ICompiledGraph,
    runConfig: dataform.IRunConfig,
    state: dataform.IWarehouseState
  ) {
    this.compiledGraph = compiledGraph;
    this.runConfig = runConfig;
    this.state = state;
    this.adapter = adapters.create(compiledGraph.projectConfig);
  }

  public build(): dataform.ExecutionGraph {
    if (utils.graphHasErrors(this.compiledGraph)) {
      throw Error(`Project has unresolved compilation or validation errors.`);
    }

    const tableStateByTarget: { [targetJson: string]: dataform.ITableMetadata } = {};
    this.state.tables.forEach(tableState => {
      tableStateByTarget[JSON.stringify(tableState.target)] = tableState;
    });

    // Remove inline tables.
    const filteredTables = this.compiledGraph.tables.filter(t => t.type !== "inline");

    // Firstly, turn every thing into an execution node.
    const allNodes: dataform.IExecutionNode[] = [].concat(
      filteredTables.map(t => this.buildTable(t, tableStateByTarget[JSON.stringify(t.target)])),
      this.compiledGraph.operations.map(o => this.buildOperation(o)),
      this.compiledGraph.assertions.map(a => this.buildAssertion(a))
    );
    const allNodeNames = allNodes.map(n => n.name);
    const nodeNameMap: { [name: string]: dataform.IExecutionNode } = {};
    allNodes.forEach(node => (nodeNameMap[node.name] = node));

    // Determine which nodes should be included.
    const includedNodeNames =
      this.runConfig.nodes && this.runConfig.nodes.length > 0
        ? utils.matchPatterns(this.runConfig.nodes, allNodeNames)
        : allNodeNames;
    let includedNodes = allNodes.filter(node => includedNodeNames.indexOf(node.name) >= 0);
    if (this.runConfig.includeDependencies) {
      // Compute all transitive dependencies.
      for (let i = 0; i < allNodes.length; i++) {
        includedNodes.forEach(node => {
          const matchingNodeNames =
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
    return dataform.ExecutionGraph.create({
      projectConfig: this.compiledGraph.projectConfig,
      runConfig: this.runConfig,
      warehouseState: this.state,
      nodes: includedNodes
    });
  }

  public buildTable(t: dataform.ITable, tableMetadata: dataform.ITableMetadata) {
    const emptyTasks = [] as dataform.IExecutionTask[];

    const tasks = t.disabled
      ? emptyTasks
      : emptyTasks.concat(
          (t.preOps || []).map(pre => ({ statement: pre })),
          this.adapter.publishTasks(t, this.runConfig, tableMetadata).build(),
          (t.postOps || []).map(post => ({ statement: post }))
        );

    return dataform.ExecutionNode.create({
      name: t.name,
      dependencies: t.dependencies,
      type: "table",
      target: t.target,
      tableType: t.type,
      tasks
    });
  }

  public buildOperation(operation: dataform.IOperation) {
    return dataform.ExecutionNode.create({
      name: operation.name,
      dependencies: operation.dependencies,
      type: "operation",
      target: operation.target,
      tasks: operation.queries.map(statement => ({
        type: "statement",
        statement
      }))
    });
  }

  public buildAssertion(assertion: dataform.IAssertion) {
    return dataform.ExecutionNode.create({
      name: assertion.name,
      dependencies: assertion.dependencies,
      type: "assertion",
      tasks: this.adapter.assertTasks(assertion, this.compiledGraph.projectConfig).build()
    });
  }
}
