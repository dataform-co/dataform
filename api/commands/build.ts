import { prune } from "df/api/commands/prune";
import { state } from "df/api/commands/state";
import * as dbadapters from "df/api/dbadapters";
import { actionsByTarget } from "df/api/utils/graphs";
import { JSONObjectStringifier, StringifiedMap } from "df/common/strings/stringifier";
import { adapters } from "df/core";
import { IActionProto } from "df/core/session";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  dbadapter: dbadapters.IDbAdapter
) {
  const prunedGraph = prune(compiledGraph, runConfig);
  const stateResult = await state(prunedGraph, dbadapter);
  return new Builder(prunedGraph, actionsByTarget(compiledGraph), runConfig, stateResult).build();
}

export class Builder {
  private readonly adapter: adapters.IAdapter;

  constructor(
    private readonly prunedGraph: dataform.ICompiledGraph,
    private readonly allActions: StringifiedMap<dataform.ITarget, IActionProto>,
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

    const transitiveInputsByTarget = new StringifiedMap<dataform.ITarget, dataform.ITarget[]>(
      JSONObjectStringifier.create()
    );
    const actions: dataform.IExecutionAction[] = [].concat(
      this.prunedGraph.tables.map(t =>
        this.buildTable(t, tableMetadataByTarget.get(t.target), transitiveInputsByTarget)
      ),
      this.prunedGraph.operations.map(o => this.buildOperation(o, transitiveInputsByTarget)),
      this.prunedGraph.assertions.map(a => this.buildAssertion(a, transitiveInputsByTarget))
    );
    return dataform.ExecutionGraph.create({
      projectConfig: this.prunedGraph.projectConfig,
      runConfig: {
        ...this.runConfig,
        useRunCache:
          !this.runConfig.hasOwnProperty("useRunCache") ||
          typeof this.runConfig.useRunCache === "undefined"
            ? this.prunedGraph.projectConfig.useRunCache
            : this.runConfig.useRunCache
      },
      warehouseState: this.warehouseState,
      actions
    });
  }

  private buildTable(
    table: dataform.ITable,
    tableMetadata: dataform.ITableMetadata,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, dataform.ITarget[]>
  ) {
    if (table.protected && this.runConfig.fullRefresh) {
      throw new Error("Protected datasets cannot be fully refreshed.");
    }

    const tasks = table.disabled
      ? ([] as dataform.IExecutionTask[])
      : this.adapter.publishTasks(table, this.runConfig, tableMetadata).build();

    return dataform.ExecutionAction.create({
      name: table.name,
      transitiveInputs: this.getAllTransitiveInputs(table, transitiveInputsByTarget),
      dependencies: table.dependencies,
      type: "table",
      target: table.target,
      tableType: table.type,
      tasks,
      fileName: table.fileName,
      actionDescriptor: table.actionDescriptor
    });
  }

  private buildOperation(
    operation: dataform.IOperation,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, dataform.ITarget[]>
  ) {
    return dataform.ExecutionAction.create({
      name: operation.name,
      transitiveInputs: this.getAllTransitiveInputs(operation, transitiveInputsByTarget),
      dependencies: operation.dependencies,
      type: "operation",
      target: operation.target,
      tasks: operation.queries.map(statement => ({ type: "statement", statement })),
      fileName: operation.fileName,
      actionDescriptor: operation.actionDescriptor
    });
  }

  private buildAssertion(
    assertion: dataform.IAssertion,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, dataform.ITarget[]>
  ) {
    return dataform.ExecutionAction.create({
      name: assertion.name,
      transitiveInputs: this.getAllTransitiveInputs(assertion, transitiveInputsByTarget),
      dependencies: assertion.dependencies,
      type: "assertion",
      target: assertion.target,
      tasks: this.adapter.assertTasks(assertion, this.prunedGraph.projectConfig).build(),
      fileName: assertion.fileName,
      actionDescriptor: assertion.actionDescriptor
    });
  }

  private getAllTransitiveInputs(
    action: dataform.ITable | dataform.IOperation | dataform.IAssertion,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, dataform.ITarget[]>
  ) {
    if (!action.target) {
      return [];
    }
    if (!transitiveInputsByTarget.has(action.target)) {
      let transitiveInputTargets = action.dependencyTargets;
      for (const transitiveInputTarget of action.dependencyTargets || []) {
        const transitiveInputAction = this.allActions.get(transitiveInputTarget);
        // Recursively add transitive inputs for all dependencies that are not tables or declarations.
        // (i.e. recurse through all dependency views, operations, etc.)
        if (
          !(
            (transitiveInputAction instanceof dataform.Table &&
              ["table", "incremental"].includes(transitiveInputAction.type)) ||
            transitiveInputAction instanceof dataform.Declaration
          )
        ) {
          transitiveInputTargets = transitiveInputTargets.concat(
            this.getAllTransitiveInputs(transitiveInputAction, transitiveInputsByTarget)
          );
        }
      }
      transitiveInputsByTarget.set(action.target, transitiveInputTargets);
    }
    return transitiveInputsByTarget.get(action.target);
  }
}
