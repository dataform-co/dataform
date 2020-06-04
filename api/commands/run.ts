import * as dbadapters from "df/api/dbadapters";
import { retry } from "df/api/utils/retry";
import { hashExecutionAction } from "df/api/utils/run_cache";
import { JSONObjectStringifier, StringifiedMap } from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";
import EventEmitter from "events";
import Long from "long";

const CANCEL_EVENT = "jobCancel";

const isSuccessfulAction = (actionResult: dataform.IActionResult) =>
  actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.DISABLED;

export function run(graph: dataform.IExecutionGraph, dbadapter: dbadapters.IDbAdapter): Runner {
  return Runner.create(dbadapter, graph).execute();
}

export class Runner {
  public static create(dbadapter: dbadapters.IDbAdapter, graph: dataform.IExecutionGraph) {
    return new Runner(dbadapter, graph);
  }

  private readonly warehouseStateByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.ITableMetadata
  >;
  private readonly persistedStateByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.IPersistedTableMetadata
  >;
  private readonly runResult: dataform.IRunResult = {
    actions: []
  };
  private readonly changeListeners: Array<(graph: dataform.IRunResult) => void> = [];
  private readonly eEmitter: EventEmitter;

  private pendingActions: dataform.IExecutionAction[];
  private cancelled = false;
  private timeout: NodeJS.Timer;
  private timedOut = false;
  private executionTask: Promise<dataform.IRunResult>;

  constructor(
    private readonly dbadapter: dbadapters.IDbAdapter,
    private readonly graph: dataform.IExecutionGraph
  ) {
    this.warehouseStateByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      graph.warehouseState.tables.map(tableMetadata => [tableMetadata.target, tableMetadata])
    );
    this.persistedStateByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      graph.warehouseState.cachedStates?.map(persistedTableMetadata => [
        persistedTableMetadata.target,
        persistedTableMetadata
      ])
    );

    this.pendingActions = graph.actions;
    this.eEmitter = new EventEmitter();
    // There could feasibly be thousands of listeners to this, 0 makes the limit infinite.
    this.eEmitter.setMaxListeners(0);
  }

  public onChange(listener: (graph: dataform.IRunResult) => Promise<void> | void): Runner {
    this.changeListeners.push(listener);
    return this;
  }

  public execute(): this {
    if (!!this.executionTask) {
      throw new Error("Executor already started.");
    }
    this.executionTask = this.executeGraph();
    if (!!this.graph.runConfig && !!this.graph.runConfig.timeoutMillis) {
      this.timeout = setTimeout(() => {
        this.timedOut = true;
        this.cancel();
      }, this.graph.runConfig.timeoutMillis);
    }
    return this;
  }

  public cancel() {
    this.cancelled = true;
    this.eEmitter.emit(CANCEL_EVENT);
  }

  public async result(): Promise<dataform.IRunResult> {
    try {
      return await this.executionTask;
    } finally {
      if (!!this.timeout) {
        clearTimeout(this.timeout);
      }
    }
  }

  private triggerChange() {
    return Promise.all(this.changeListeners.map(listener => listener(this.runResult)));
  }

  private async executeGraph() {
    const timer = Timer.start();

    this.runResult.status = dataform.RunResult.ExecutionStatus.RUNNING;
    this.runResult.timing = timer.current();
    await this.triggerChange();

    await this.prepareAllSchemas();

    // Recursively execute all actions as they become executable.
    await this.executeAllActionsReadyForExecution();

    this.runResult.timing = timer.end();

    if (this.graph.runConfig && this.graph.runConfig.useRunCache) {
      await this.dbadapter.prepareStateMetadataTable();

      await this.dbadapter.deleteStateMetadata(this.graph.actions);

      const successfulActions = this.runResult.actions
        .filter(action =>
          [
            dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
            dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
          ].includes(action.status)
        )
        .map(action =>
          this.graph.actions.find(executionAction => action.name === executionAction.name)
        );

      // Currently, we don't support caching for operations (and any dependents)
      await this.dbadapter.persistStateMetadata(
        successfulActions.filter(action => action.type !== "operation")
      );
    }

    this.runResult.status = dataform.RunResult.ExecutionStatus.SUCCESSFUL;
    if (this.timedOut) {
      this.runResult.status = dataform.RunResult.ExecutionStatus.TIMED_OUT;
    } else if (this.cancelled) {
      this.runResult.status = dataform.RunResult.ExecutionStatus.CANCELLED;
    } else if (
      this.runResult.actions.some(
        action => action.status === dataform.ActionResult.ExecutionStatus.FAILED
      )
    ) {
      this.runResult.status = dataform.RunResult.ExecutionStatus.FAILED;
    }

    return this.runResult;
  }

  private async prepareAllSchemas() {
    // Work out all the schemas we are going to need to create first.
    const databaseSchemas = new Map<string, Set<string>>();
    this.graph.actions
      .filter(action => !!action.target && !!action.target.schema)
      .forEach(({ target }) => {
        // This field may not be present for older versions of dataform.
        const trueDatabase = target.database || this.graph.projectConfig.defaultDatabase;
        if (!databaseSchemas.has(target.database)) {
          databaseSchemas.set(trueDatabase, new Set<string>());
        }
        databaseSchemas.get(trueDatabase).add(target.schema);
      });

    if (!databaseSchemas.has(this.graph.projectConfig.defaultDatabase)) {
      databaseSchemas.set(this.graph.projectConfig.defaultDatabase, new Set<string>());
    }

    if (this.graph.projectConfig.useRunCache) {
      databaseSchemas
        .get(this.graph.projectConfig.defaultDatabase)
        .add(dbadapters.CACHED_STATE_TABLE_TARGET.schema);
    }

    // Wait for all schemas to be created.
    await Promise.all(
      Array.from(databaseSchemas).map(([database, schemas]) =>
        Promise.all(
          Array.from(schemas).map(schema => this.dbadapter.prepareSchema(database, schema))
        )
      )
    );
  }
  private async executeAllActionsReadyForExecution() {
    // If the run has been cancelled, cancel all pending actions.
    if (this.cancelled) {
      const allPendingActions = this.pendingActions;
      this.pendingActions = [];
      allPendingActions.forEach(pendingAction =>
        this.runResult.actions.push({
          name: pendingAction.name,
          status: dataform.ActionResult.ExecutionStatus.SKIPPED,
          tasks: pendingAction.tasks.map(() => ({
            status: dataform.TaskResult.ExecutionStatus.SKIPPED
          }))
        })
      );
      await this.triggerChange();
      return;
    }

    const executableActions = this.removeExecutableActionsFromPending();
    const skippableActions = this.removeSkippableActionsFromPending();

    skippableActions.forEach(skippableAction => {
      this.runResult.actions.push({
        name: skippableAction.name,
        status: dataform.ActionResult.ExecutionStatus.SKIPPED,
        tasks: skippableAction.tasks.map(() => ({
          status: dataform.TaskResult.ExecutionStatus.SKIPPED
        }))
      });
    });
    const onActionsSkipped = async () => {
      if (skippableActions.length > 0) {
        await this.triggerChange();
        await this.executeAllActionsReadyForExecution();
      }
    };

    await Promise.all([
      onActionsSkipped(),
      Promise.all(
        executableActions.map(async executableAction => {
          await this.executeAction(executableAction);
          await this.executeAllActionsReadyForExecution();
        })
      )
    ]);
  }

  private removeExecutableActionsFromPending() {
    const allDependenciesHaveExecutedSuccessfully = allDependenciesHaveBeenExecuted(
      this.runResult.actions.filter(isSuccessfulAction)
    );
    const executableActions = this.pendingActions.filter(allDependenciesHaveExecutedSuccessfully);
    this.pendingActions = this.pendingActions.filter(
      action => !allDependenciesHaveExecutedSuccessfully(action)
    );
    return executableActions;
  }

  private removeSkippableActionsFromPending() {
    const allDependenciesHaveExecuted = allDependenciesHaveBeenExecuted(
      this.runResult.actions.filter(
        action => action.status !== dataform.ActionResult.ExecutionStatus.RUNNING
      )
    );
    const skippableActions = this.pendingActions.filter(allDependenciesHaveExecuted);
    this.pendingActions = this.pendingActions.filter(
      action => !allDependenciesHaveExecuted(action)
    );
    return skippableActions;
  }

  private async executeAction(action: dataform.IExecutionAction): Promise<void> {
    if (action.tasks.length === 0) {
      this.runResult.actions.push({
        name: action.name,
        status: dataform.ActionResult.ExecutionStatus.DISABLED,
        tasks: []
      });
      await this.triggerChange();
      return;
    }

    if (this.shouldCacheSkip(action)) {
      this.runResult.actions.push({
        name: action.name,
        status: dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
        tasks: []
      });
      await this.triggerChange();
      return;
    }

    const timer = Timer.start();
    const actionResult: dataform.IActionResult = {
      name: action.name,
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      timing: timer.current(),
      tasks: []
    };
    this.runResult.actions.push(actionResult);
    await this.triggerChange();

    for (const task of action.tasks) {
      if (
        actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
        !this.cancelled
      ) {
        const taskStatus = await this.executeTask(task, actionResult);
        if (taskStatus === dataform.TaskResult.ExecutionStatus.FAILED) {
          actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
        } else if (taskStatus === dataform.TaskResult.ExecutionStatus.CANCELLED) {
          actionResult.status = dataform.ActionResult.ExecutionStatus.CANCELLED;
        }
      } else {
        actionResult.tasks.push({
          status: dataform.TaskResult.ExecutionStatus.SKIPPED
        });
      }
    }

    if (actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
    }

    if (
      action.actionDescriptor &&
      actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL &&
      !(this.graph.runConfig && this.graph.runConfig.disableSetMetadata)
    ) {
      await this.dbadapter.setMetadata(action);
    }
    actionResult.timing = timer.end();
    await this.triggerChange();
  }

  private async executeTask(
    task: dataform.IExecutionTask,
    parentAction: dataform.IActionResult
  ): Promise<dataform.TaskResult.ExecutionStatus> {
    const timer = Timer.start();
    const taskResult: dataform.ITaskResult = {
      status: dataform.TaskResult.ExecutionStatus.RUNNING,
      timing: timer.current(),
      metadata: {}
    };
    parentAction.tasks.push(taskResult);
    await this.triggerChange();
    try {
      // Retry this function a given number of times, configurable by user
      const { rows, metadata } = await retry(
        () =>
          this.dbadapter.execute(task.statement, {
            onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel),
            maxResults: 1
          }),
        task.type === "operation" ? 0 : this.graph.projectConfig.idempotentActionRetries || 0
      );
      taskResult.metadata = metadata;
      if (task.type === "assertion") {
        // We expect that an assertion query returns 1 row, with 1 field that is the row count.
        // We don't really care what that field/column is called.
        const rowCount = rows[0][Object.keys(rows[0])[0]];
        if (rowCount > 0) {
          throw new Error(`Assertion failed: query returned ${rowCount} row(s).`);
        }
      }
      taskResult.status = dataform.TaskResult.ExecutionStatus.SUCCESSFUL;
    } catch (e) {
      taskResult.status = this.cancelled
        ? dataform.TaskResult.ExecutionStatus.CANCELLED
        : dataform.TaskResult.ExecutionStatus.FAILED;
      taskResult.errorMessage = `${this.graph.projectConfig.warehouse} error: ${e.message}`;
    }
    taskResult.timing = timer.end();
    await this.triggerChange();
    return taskResult.status;
  }

  private shouldCacheSkip(executionAction: dataform.IExecutionAction): boolean {
    // Run caching must be turned on.
    if (!this.graph.runConfig?.useRunCache) {
      return false;
    }

    // If the action is non-hermetic, always run it.
    if (executionAction.hermeticity === dataform.ActionHermeticity.NON_HERMETIC) {
      return false;
    }

    // If the action is of type "operation", always run it.
    if (executionAction.type === "operation") {
      return false;
    }

    // All dependencies, if they were included in the run, must have completed with
    // CACHE_SKIPPED status.
    for (const dependencyTarget of executionAction.transitiveInputs) {
      const dependencyAction = this.graph.actions.find(
        action =>
          action.target.database === dependencyTarget.database &&
          action.target.schema === dependencyTarget.schema &&
          action.target.name === dependencyTarget.name
      );
      if (!dependencyAction) {
        continue;
      }

      if (
        this.runResult.actions.find(action => action.name === dependencyAction.name).status !==
        dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
      ) {
        return false;
      }
    }

    // Persisted state for this action must exist, and the persisted action hash must match this action's hash.
    if (!this.persistedStateByTarget.has(executionAction.target)) {
      return false;
    }
    const persistedTableMetadata = this.persistedStateByTarget.get(executionAction.target);
    // tslint:disable-next-line: tsr-detect-possible-timing-attacks
    if (hashExecutionAction(executionAction) !== persistedTableMetadata.definitionHash) {
      return false;
    }

    // The target table for this action must exist, and the table metadata's last update timestamp must match
    // the persisted last update timestamp.
    if (!this.warehouseStateByTarget.has(executionAction.target)) {
      return false;
    }
    if (
      persistedTableMetadata.lastUpdatedMillis.notEquals(
        this.warehouseStateByTarget.get(executionAction.target).lastUpdatedMillis
      )
    ) {
      return false;
    }

    // All transitive inputs' last change timestamps must match the corresponding timestamps stored
    // in persisted state.
    const persistedTransitiveInputUpdateTimestamps = new StringifiedMap(
      JSONObjectStringifier.create<dataform.ITarget>(),
      persistedTableMetadata.transitiveInputTables.map(transitiveInputTable => [
        transitiveInputTable.target,
        transitiveInputTable.lastUpdatedMillis
      ])
    );
    for (const transitiveInput of executionAction.transitiveInputs) {
      if (!persistedTransitiveInputUpdateTimestamps.has(transitiveInput)) {
        return false;
      }
      const persistedTransitiveInputUpdateTimestamp = persistedTransitiveInputUpdateTimestamps.get(
        transitiveInput
      );
      if (!this.warehouseStateByTarget.has(transitiveInput)) {
        return false;
      }
      const latestTransitiveInputUpdateTimestamp = this.warehouseStateByTarget.get(transitiveInput)
        .lastUpdatedMillis;
      if (persistedTransitiveInputUpdateTimestamp.notEquals(latestTransitiveInputUpdateTimestamp)) {
        return false;
      }
    }

    return true;
  }
}

function allDependenciesHaveBeenExecuted(actionResults: dataform.IActionResult[]) {
  const executedActionNames = actionResults.map(action => action.name);
  return (action: dataform.IExecutionAction) => {
    for (const dependency of action.dependencies) {
      if (!executedActionNames.includes(dependency)) {
        return false;
      }
    }
    return true;
  };
}

class Timer {
  public static start() {
    return new Timer(new Date().valueOf());
  }
  private constructor(readonly startTimeMillis: number) {}

  public current(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis)
    };
  }

  public end(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
      endTimeMillis: Long.fromNumber(new Date().valueOf())
    };
  }
}
