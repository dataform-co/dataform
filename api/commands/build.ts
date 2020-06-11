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
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

export async function build(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
  dbadapter: dbadapters.IDbAdapter
) {
  runConfig = {
    ...runConfig,
    useRunCache:
      runConfig.hasOwnProperty("useRunCache") && typeof runConfig.useRunCache !== "undefined"
        ? runConfig.useRunCache
        : compiledGraph.projectConfig.useRunCache,
    useSingleQueryPerAction:
      runConfig.hasOwnProperty("useSingleQueryPerAction") &&
      typeof runConfig.useSingleQueryPerAction !== "undefined"
        ? runConfig.useSingleQueryPerAction
        : compiledGraph.projectConfig.useSingleQueryPerAction
  };

  const prunedGraph = prune(compiledGraph, runConfig);
  const transitiveInputsByTarget = computeAllTransitiveInputs(compiledGraph);

  const allInvolvedTargets = new StringifiedSet<dataform.ITarget>(JSONObjectStringifier.create());
  for (const includedAction of [
    ...prunedGraph.tables,
    ...prunedGraph.operations,
    ...prunedGraph.assertions
  ]) {
    allInvolvedTargets.add(includedAction.target);
    if (versionValidForTransitiveInputs(compiledGraph)) {
      transitiveInputsByTarget
        .get(includedAction.target)
        .forEach(transitiveInputTarget => allInvolvedTargets.add(transitiveInputTarget));
    }
  }

  return new Builder(
    prunedGraph,
    runConfig,
    await state(dbadapter, Array.from(allInvolvedTargets), runConfig.useRunCache),
    transitiveInputsByTarget
  ).build();
}

export class Builder {
  private readonly adapter: adapters.IAdapter;

  constructor(
    private readonly prunedGraph: dataform.ICompiledGraph,
    private readonly runConfig: dataform.IRunConfig,
    private readonly warehouseState: dataform.IWarehouseState,
    private readonly transitiveInputsByTarget: StringifiedMap<
      dataform.ITarget,
      StringifiedSet<dataform.ITarget>
    >
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

    const transitiveInputsByTarget = new StringifiedMap<
      dataform.ITarget,
      StringifiedSet<dataform.ITarget>
    >(JSONObjectStringifier.create());
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

    const tasks = table.disabled
      ? ([] as dataform.IExecutionTask[])
      : this.adapter.publishTasks(table, runConfig, tableMetadata).build();

    return {
      ...this.toPartialExecutionAction(table),
      type: "table",
      tableType: table.type,
      tasks,
      hermeticity: table.hermeticity || dataform.ActionHermeticity.HERMETIC
    };
  }

  private buildOperation(operation: dataform.IOperation) {
    return {
      ...this.toPartialExecutionAction(operation),
      type: "operation",
      tasks: operation.queries.map(statement => ({ type: "statement", statement })),
      hermeticity: operation.hermeticity || dataform.ActionHermeticity.NON_HERMETIC
    };
  }

  private buildAssertion(assertion: dataform.IAssertion) {
    return {
      ...this.toPartialExecutionAction(assertion),
      type: "assertion",
      tasks: this.adapter.assertTasks(assertion, this.prunedGraph.projectConfig).build(),
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
      transitiveInputs: versionValidForTransitiveInputs(this.prunedGraph)
        ? Array.from(this.transitiveInputsByTarget.get(action.target))
        : [],
      actionDescriptor: action.actionDescriptor
    });
  }
}

function versionValidForTransitiveInputs(compiledGraph: dataform.ICompiledGraph) {
  return (
    compiledGraph.dataformCoreVersion && semver.gte(compiledGraph.dataformCoreVersion, "1.6.11")
  );
}

export function computeAllTransitiveInputs(compiledGraph: dataform.ICompiledGraph) {
  const transitiveInputsByTarget = new StringifiedMap<
    dataform.ITarget,
    StringifiedSet<dataform.ITarget>
  >(JSONObjectStringifier.create());

  if (!versionValidForTransitiveInputs(compiledGraph)) {
    return transitiveInputsByTarget;
  }

  const actionsByTargetMap = actionsByTarget(compiledGraph);
  for (const action of [
    ...compiledGraph.tables,
    ...compiledGraph.operations,
    ...compiledGraph.assertions
  ]) {
    if (!transitiveInputsByTarget.has(action.target)) {
      transitiveInputsByTarget.set(
        action.target,
        computeTransitiveInputsForAction(action, actionsByTargetMap, transitiveInputsByTarget)
      );
    }
  }

  return transitiveInputsByTarget;
}

function computeTransitiveInputsForAction(
  action: dataform.ITable | dataform.IOperation | dataform.IAssertion,
  actionByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.IAssertion | dataform.ITable | dataform.IOperation | dataform.IDeclaration
  >,
  transitiveInputsByTarget: StringifiedMap<dataform.ITarget, StringifiedSet<dataform.ITarget>>
) {
  const transitiveInputTargets = new StringifiedSet(JSONObjectStringifier.create());
  if (!transitiveInputsByTarget.has(action.target)) {
    for (const transitiveInputTarget of action.dependencyTargets || []) {
      transitiveInputTargets.add(transitiveInputTarget);
      const transitiveInputAction = actionByTarget.get(transitiveInputTarget);
      // Recursively add transitive inputs for all dependencies that are not tables or declarations.
      // (i.e. recurse through all dependency views, operations, etc.)
      if (
        !(
          (transitiveInputAction instanceof dataform.Table &&
            ["table", "incremental"].includes(transitiveInputAction.type)) ||
          transitiveInputAction instanceof dataform.Declaration
        )
      ) {
        computeTransitiveInputsForAction(
          transitiveInputAction,
          actionByTarget,
          transitiveInputsByTarget
        ).forEach(target => transitiveInputTargets.add(target));
      }
    }
    transitiveInputsByTarget.set(action.target, transitiveInputTargets);
  }
  return transitiveInputsByTarget.get(action.target);
}
