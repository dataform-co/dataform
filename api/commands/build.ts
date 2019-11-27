import { Credentials } from "@dataform/api/commands/credentials";
import { state } from "@dataform/api/commands/state";
import * as dbadapters from "@dataform/api/dbadapters";
import { adapters } from "@dataform/core";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { prune } from "df/api/commands/prune";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  credentials: Credentials
) {
  const prunedGraph = prune(compiledGraph, runConfig);
  const stateResult = await state(
    prunedGraph,
    dbadapters.create(credentials, compiledGraph.projectConfig.warehouse)
  );
  return new Builder(prunedGraph, runConfig, stateResult).build();
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
      throw new Error(`Project has unresolved compilation or validation errors.`);
    }

    const tableStateByTarget: { [targetJson: string]: dataform.ITableMetadata } = {};
    this.state.tables.forEach(tableState => {
      tableStateByTarget[JSON.stringify(tableState.target)] = tableState;
    });

    const actions: dataform.IExecutionAction[] = [].concat(
      this.compiledGraph.tables.map(t =>
        this.buildTable(t, tableStateByTarget[JSON.stringify(t.target)])
      ),
      this.compiledGraph.operations.map(o => this.buildOperation(o)),
      this.compiledGraph.assertions.map(a => this.buildAssertion(a))
    );

    return dataform.ExecutionGraph.create({
      projectConfig: this.compiledGraph.projectConfig,
      runConfig: this.runConfig,
      warehouseState: this.state,
      actions
    });
  }

  public buildTable(t: dataform.ITable, tableMetadata: dataform.ITableMetadata) {
    const emptyTasks = [] as dataform.IExecutionTask[];

    if (t.protected && this.runConfig.fullRefresh) {
      throw new Error("Protected datasets cannot be fully refreshed.");
    }

    const tasks = t.disabled
      ? emptyTasks
      : emptyTasks.concat(
          (t.preOps || []).map(pre => ({ statement: pre })),
          this.adapter.publishTasks(t, this.runConfig, tableMetadata).build(),
          (t.postOps || []).map(post => ({ statement: post }))
        );

    return dataform.ExecutionAction.create({
      name: t.name,
      dependencies: t.dependencies,
      type: "table",
      target: t.target,
      tableType: t.type,
      tasks
    });
  }

  public buildOperation(operation: dataform.IOperation) {
    return dataform.ExecutionAction.create({
      name: operation.name,
      dependencies: operation.dependencies,
      type: "operation",
      target: operation.target,
      tasks: operation.queries.map(statement => ({ type: "statement", statement }))
    });
  }

  public buildAssertion(assertion: dataform.IAssertion) {
    return dataform.ExecutionAction.create({
      name: assertion.name,
      dependencies: assertion.dependencies,
      type: "assertion",
      target: assertion.target,
      tasks: this.adapter.assertTasks(assertion, this.compiledGraph.projectConfig).build()
    });
  }
}
