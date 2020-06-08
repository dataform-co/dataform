import * as semver from "semver";

import { prune } from "df/api/commands/prune";
import { state } from "df/api/commands/state";
import * as dbadapters from "df/api/dbadapters";
import { actionsByTarget } from "df/api/utils/graphs";
import {
  JSONObjectStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
import { adapters } from "df/core";
import { IActionProto } from "df/core/session";
import { Tasks } from "df/core/tasks";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  dbadapter: dbadapters.IDbAdapter
) {
  const stateResult = await state(compiledGraph, dbadapter);
  return new Builder(compiledGraph, actionsByTarget(compiledGraph), runConfig, stateResult).build();
}

export class Builder {
  private readonly adapter: adapters.IAdapter;

  constructor(
    private readonly compiledGraph: dataform.ICompiledGraph,
    private readonly allActions: StringifiedMap<dataform.ITarget, IActionProto>,
    private readonly runConfig: dataform.IRunConfig,
    private readonly warehouseState: dataform.IWarehouseState
  ) {
    this.adapter = adapters.create(
      compiledGraph.projectConfig,
      compiledGraph.dataformCoreVersion || "1.0.0"
    );
  }

  public build(): dataform.ExecutionGraph {
    if (utils.graphHasErrors(this.compiledGraph)) {
      throw new Error(`Project has unresolved compilation or validation errors.`);
    }

    const tableMetadataByTarget = new StringifiedMap<dataform.ITarget, dataform.ITableMetadata>(
      JSONObjectStringifier.create()
    );
    this.warehouseState.tables.forEach(tableState => {
      tableMetadataByTarget.set(tableState.target, tableState);
    });

    const runConfig: dataform.IRunConfig = {
      ...this.runConfig,
      useRunCache:
        !this.runConfig.hasOwnProperty("useRunCache") ||
        typeof this.runConfig.useRunCache === "undefined"
          ? this.compiledGraph.projectConfig.useRunCache
          : this.runConfig.useRunCache,
      useSingleQueryPerAction:
        !this.compiledGraph.projectConfig?.hasOwnProperty("useSingleQueryPerAction") ||
        typeof this.compiledGraph.projectConfig?.useSingleQueryPerAction === "undefined"
          ? this.compiledGraph.projectConfig.useSingleQueryPerAction
          : this.compiledGraph.projectConfig.useSingleQueryPerAction
    };

    const prunedGraph = prune(this.compiledGraph, this.runConfig);
    const transitiveInputsByTarget = new StringifiedMap<
      dataform.ITarget,
      StringifiedSet<dataform.ITarget>
    >(JSONObjectStringifier.create());
    const actions: dataform.IExecutionAction[] = [].concat(
      prunedGraph.tables.map(t =>
        this.buildTable(t, tableMetadataByTarget.get(t.target), transitiveInputsByTarget, runConfig)
      ),
      prunedGraph.operations.map(o => this.buildOperation(o, transitiveInputsByTarget)),
      prunedGraph.assertions.map(a => this.buildAssertion(a, transitiveInputsByTarget))
    );
    return dataform.ExecutionGraph.create({
      projectConfig: this.compiledGraph.projectConfig,
      runConfig,
      warehouseState: this.warehouseState,
      actions
    });
  }

  private buildTable(
    table: dataform.ITable,
    tableMetadata: dataform.ITableMetadata,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>,
    runConfig: dataform.IRunConfig
  ) {
    if (table.protected && this.runConfig.fullRefresh) {
      throw new Error("Protected datasets cannot be fully refreshed.");
    }

    const tasks = table.disabled
      ? ([] as dataform.IExecutionTask[])
      : this.adapter.publishTasks(table, runConfig, tableMetadata).build();

    return {
      ...this.toPartialExecutionAction(table, transitiveInputsByTarget),
      type: "table",
      tableType: table.type,
      tasks
    };
  }

  private buildOperation(
    operation: dataform.IOperation,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>
  ) {
    return {
      ...this.toPartialExecutionAction(operation, transitiveInputsByTarget),
      type: "operation",
      tasks: operation.queries.map(statement => ({ type: "statement", statement }))
    };
  }

  private buildAssertion(
    assertion: dataform.IAssertion,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>
  ) {
    return {
      ...this.toPartialExecutionAction(assertion, transitiveInputsByTarget),
      type: "assertion",
      tasks: this.adapter.assertTasks(assertion, this.compiledGraph.projectConfig).build()
    };
  }

  private toPartialExecutionAction(
    action: dataform.ITable | dataform.IOperation | dataform.IAssertion,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>
  ) {
    return dataform.ExecutionAction.create({
      name: action.name,
      target: action.target,
      fileName: action.fileName,
      dependencies: action.dependencies,
      transitiveInputs: Array.from(this.getAllTransitiveInputs(action, transitiveInputsByTarget)),
      hermeticity: action.hermeticity,
      actionDescriptor: action.actionDescriptor
    });
  }

  private getAllTransitiveInputs(
    action: dataform.ITable | dataform.IOperation | dataform.IAssertion,
    transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>
  ): StringifiedSet<dataform.ITarget> {
    const transitiveInputTargets = new StringifiedSet(JSONObjectStringifier.create());
    if (
      !this.compiledGraph.dataformCoreVersion ||
      semver.lt(this.compiledGraph.dataformCoreVersion, "1.6.11")
    ) {
      return transitiveInputTargets;
    }
    if (!transitiveInputsByTarget.has(action.target)) {
      for (const transitiveInputTarget of action.dependencyTargets || []) {
        transitiveInputTargets.add(transitiveInputTarget);
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
          this.getAllTransitiveInputs(
            transitiveInputAction,
            transitiveInputsByTarget
          ).forEach(target => transitiveInputTargets.add(target));
        }
      }
      transitiveInputsByTarget.set(action.target, transitiveInputTargets);
    }
    return transitiveInputsByTarget.get(action.target);
  }
}
