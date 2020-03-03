import { Credentials } from "@dataform/api/commands/credentials";
import { prune } from "@dataform/api/commands/prune";
import { state } from "@dataform/api/commands/state";
import * as dbadapters from "@dataform/api/dbadapters";
import { adapters } from "@dataform/core";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  credentials: Credentials
) {
  const prunedGraph = prune(compiledGraph, runConfig);
  const dbadapter = dbadapters.create(credentials, compiledGraph.projectConfig.warehouse);
  try {
    const stateResult = await state(prunedGraph, dbadapter);
    return new Builder(prunedGraph, runConfig, stateResult).build();
  } finally {
    await dbadapter.close();
  }
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
    this.adapter = adapters.create(
      compiledGraph.projectConfig,
      compiledGraph.dataformCoreVersion || "1.0.0"
    );
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
      runConfig: {
        ...this.runConfig,
        useRunCache:
          !this.runConfig.hasOwnProperty("useRunCache") ||
          typeof this.runConfig.useRunCache === "undefined"
            ? this.compiledGraph.projectConfig.useRunCache
            : this.runConfig.useRunCache
      },
      warehouseState: this.state,
      actions
    });
  }

  public buildTable(t: dataform.ITable, tableMetadata: dataform.ITableMetadata) {
    if (t.protected && this.runConfig.fullRefresh) {
      throw new Error("Protected datasets cannot be fully refreshed.");
    }

    const tasks = t.disabled
      ? ([] as dataform.IExecutionTask[])
      : this.adapter.publishTasks(t, this.runConfig, tableMetadata).build();

    return dataform.ExecutionAction.create({
      name: t.name,
      dependencyTargets: t.dependencyTargets,
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
      dependencyTargets: operation.dependencyTargets,
      dependencies: operation.dependencies,
      type: "operation",
      target: operation.target,
      tasks: operation.queries.map(statement => ({ type: "statement", statement }))
    });
  }

  public buildAssertion(assertion: dataform.IAssertion) {
    return dataform.ExecutionAction.create({
      name: assertion.name,
      dependencyTargets: assertion.dependencyTargets,
      dependencies: assertion.dependencies,
      type: "assertion",
      target: assertion.target,
      tasks: this.adapter.assertTasks(assertion, this.compiledGraph.projectConfig).build()
    });
  }
}
