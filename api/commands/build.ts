import { prune } from "df/api/commands/prune";
import { state } from "df/api/commands/state";
import * as dbadapters from "df/api/dbadapters";
import {
  JSONObjectStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
import { adapters } from "df/core";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  dbadapter: dbadapters.IDbAdapter
) {
  runConfig = {
    ...runConfig,
    useRunCache: false
  };

  const prunedGraph = prune(compiledGraph, runConfig);

  const allInvolvedTargets = new StringifiedSet<dataform.ITarget>(
    JSONObjectStringifier.create(),
    prunedGraph.tables.map(table => table.target)
  );
  if (runConfig.useRunCache) {
    for (const includedAction of [
      ...prunedGraph.tables,
      ...prunedGraph.operations,
      ...prunedGraph.assertions
    ]) {
      allInvolvedTargets.add(includedAction.target);
    }
  }

  return new Builder(
    prunedGraph,
    runConfig,
    await state(dbadapter, Array.from(allInvolvedTargets))
  ).build();
}

export class Builder {
  private readonly adapter: adapters.IAdapter;

  constructor(
    private readonly prunedGraph: dataform.ICompiledGraph,
    private readonly runConfig: dataform.IRunConfig,
    private readonly warehouseState: dataform.IWarehouseState
  ) {
    this.adapter = adapters.create(
      prunedGraph.projectConfig,
      prunedGraph.dataformCoreVersion || "1.0.0"
    );
  }

  public build(): dataform.ExecutionGraph {
    if (utils.graphHasErrors(this.prunedGraph)) {
      throw new Error(`Project has unresolved compilation or validation errors.`);
    }

    const tableMetadataByTarget = new StringifiedMap<dataform.ITarget, dataform.ITableMetadata>(
      JSONObjectStringifier.create()
    );
    this.warehouseState.tables.forEach(tableState => {
      tableMetadataByTarget.set(tableState.target, tableState);
    });

    const actions: dataform.IExecutionAction[] = [].concat(
      this.prunedGraph.tables.map(t =>
        this.buildTable(t, tableMetadataByTarget.get(t.target), this.runConfig)
      ),
      this.prunedGraph.operations.map(o => this.buildOperation(o)),
      this.prunedGraph.assertions.map(a => this.buildAssertion(a))
    );
    return dataform.ExecutionGraph.create({
      projectConfig: this.prunedGraph.projectConfig,
      runConfig: this.runConfig,
      warehouseState: this.warehouseState,
      declarationTargets: this.prunedGraph.declarations.map(declaration => declaration.target),
      actions
    });
  }

  private buildTable(
    table: dataform.ITable,
    tableMetadata: dataform.ITableMetadata,
    runConfig: dataform.IRunConfig
  ) {
    if (table.protected && this.runConfig.fullRefresh) {
      throw new Error("Protected datasets cannot be fully refreshed.");
    }

    return {
      ...this.toPartialExecutionAction(table),
      type: "table",
      tableType: table.type,
      tasks: table.disabled
        ? []
        : this.adapter.publishTasks(table, runConfig, tableMetadata).build(),
      hermeticity: table.hermeticity || dataform.ActionHermeticity.HERMETIC
    };
  }

  private buildOperation(operation: dataform.IOperation) {
    return {
      ...this.toPartialExecutionAction(operation),
      type: "operation",
      tasks: operation.disabled
        ? []
        : operation.queries.map(statement => ({ type: "statement", statement })),
      hermeticity: operation.hermeticity || dataform.ActionHermeticity.NON_HERMETIC
    };
  }

  private buildAssertion(assertion: dataform.IAssertion) {
    return {
      ...this.toPartialExecutionAction(assertion),
      type: "assertion",
      tasks: assertion.disabled
        ? []
        : this.adapter.assertTasks(assertion, this.prunedGraph.projectConfig).build(),
      hermeticity: assertion.hermeticity || dataform.ActionHermeticity.HERMETIC
    };
  }

  private toPartialExecutionAction(
    action: dataform.ITable | dataform.IOperation | dataform.IAssertion
  ) {
    return dataform.ExecutionAction.create({
      name: action.name,
      target: action.target,
      fileName: action.fileName,
      dependencies: action.dependencies,
      actionDescriptor: action.actionDescriptor
    });
  }
}
