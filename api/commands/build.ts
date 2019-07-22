import { Credentials } from "@dataform/api/commands/credentials";
import { state } from "@dataform/api/commands/state";
import * as dbadapters from "@dataform/api/dbadapters";
import { adapters } from "@dataform/core";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

export function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  credentials: Credentials
) {
  return state(
    compiledGraph,
    dbadapters.create(credentials, compiledGraph.projectConfig.warehouse)
  ).then(state => {
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

    // Firstly, turn every thing into an execution action.
    const allActions: dataform.IExecutionAction[] = [].concat(
      filteredTables.map(t => this.buildTable(t, tableStateByTarget[JSON.stringify(t.target)])),
      this.compiledGraph.operations.map(t => this.buildOperation(t)),
      this.compiledGraph.assertions.map(t => this.buildAssertion(t))
    );
    const allActionNames = allActions.map(n => n.name);
    const actionNameMap: { [name: string]: dataform.IExecutionAction } = {};
    allActions.forEach(action => (actionNameMap[action.name] = action));

    // Actions selected with --tag option should be included
    const tagFilteredTables =
      this.runConfig.tags && this.runConfig.tags.length > 0
        ? this.compiledGraph.tables.filter(a => this.runConfig.tags.some(r => a.tags.includes(r)))
        : [];

    const tagFilteredOps =
      this.runConfig.tags && this.runConfig.tags.length > 0
        ? this.compiledGraph.operations.filter(a =>
            this.runConfig.tags.some(r => a.tags.includes(r))
          )
        : [];

    const tagFilteredAssertions =
      this.runConfig.tags && this.runConfig.tags.length > 0
        ? this.compiledGraph.assertions.filter(a =>
            this.runConfig.tags.some(r => a.tags.includes(r))
          )
        : [];

    const taggedActionNameMap: { [name: string]: dataform.IExecutionAction } = {};

    const allTaggedActions: dataform.IExecutionAction[] = [].concat(
      tagFilteredTables,
      tagFilteredOps,
      tagFilteredAssertions
    );
    allTaggedActions.forEach(action => (taggedActionNameMap[action.name] = action));
    const allTaggedActionNames = allTaggedActions.map(n => n.name);

    // Determine which action should be included.
    const includedActionNames =
      this.runConfig.actions && this.runConfig.actions.length > 0
        ? utils.matchPatterns(this.runConfig.actions, allActionNames)
        : allActionNames;

    // Merge --tags and --actions into a combined array
    let combinedActions = allActions.filter(
      action =>
        includedActionNames.indexOf(action.name) >= 0 ||
        allTaggedActionNames.indexOf(action.name) >= 0
    );
    const combinedActionNames = combinedActions.map(n => n.name);

    if (this.runConfig.includeDependencies) {
      // Compute all transitive dependencies.
      for (let i = 0; i < combinedActions.length; i++) {
        combinedActions.forEach(action => {
          const matchingActionNames =
            action.dependencies && action.dependencies.length > 0
              ? utils.matchPatterns(action.dependencies, allActionNames)
              : [];
          // Update included action names.
          matchingActionNames.forEach(actionName => {
            if (combinedActionNames.indexOf(actionName) < 0) {
              combinedActionNames.push(actionName);
            }
          });
          // Update included actions.
          combinedActions = allActions.filter(
            action => combinedActionNames.indexOf(action.name) >= 0
          );
        });
      }
    }
    // Remove any excluded dependencies.
    combinedActions.forEach(action => {
      action.dependencies = action.dependencies.filter(
        dep => includedActionNames.indexOf(dep) >= 0
      );
    });
    return dataform.ExecutionGraph.create({
      projectConfig: this.compiledGraph.projectConfig,
      runConfig: this.runConfig,
      warehouseState: this.state,
      actions: combinedActions
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
