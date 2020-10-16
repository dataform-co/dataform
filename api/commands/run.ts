import EventEmitter from "events";
import Long from "long";

import * as dbadapters from "df/api/dbadapters";
import { retry } from "df/api/utils/retry";
import { hashExecutionAction } from "df/api/utils/run_cache";
import { timingSafeEqual } from "df/common/strings";
import {
  JSONObjectStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";

const CANCEL_EVENT = "jobCancel";

const isSuccessfulAction = (actionResult: dataform.IActionResult) =>
  actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.DISABLED;

export function run(
  dbadapter: dbadapters.IDbAdapter,
  graph: dataform.IExecutionGraph,
  partiallyExecutedRunResult?: dataform.IRunResult
): Runner {
  return new Runner(dbadapter, graph, partiallyExecutedRunResult).execute();
}

export class Runner {
  private readonly warehouseStateBeforeRunByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.ITableMetadata
  >;
  private readonly warehouseStateAfterRunByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.ITableMetadata
  >;
  private readonly persistedStateByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.IPersistedTableMetadata
  >;
  private readonly nonTableDeclarationTargets: StringifiedSet<dataform.ITarget>;

  private readonly runResult: dataform.IRunResult;
  private readonly changeListeners: Array<(graph: dataform.IRunResult) => void> = [];
  private readonly eEmitter: EventEmitter;

  private pendingActions: dataform.IExecutionAction[];
  private stopped = false;
  private cancelled = false;
  private timeout: NodeJS.Timer;
  private timedOut = false;
  private executionTask: Promise<dataform.IRunResult>;

  private metadataReadPromises: Array<Promise<void>> = [];

  constructor(
    private readonly dbadapter: dbadapters.IDbAdapter,
    private readonly graph: dataform.IExecutionGraph,
    partiallyExecutedRunResult?: dataform.IRunResult
  ) {
    this.runResult = {
      actions: [],
      ...partiallyExecutedRunResult
    };
    this.warehouseStateBeforeRunByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      graph.warehouseState.tables?.map(tableMetadata => [tableMetadata.target, tableMetadata])
    );
    this.warehouseStateAfterRunByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      Array.from(this.warehouseStateBeforeRunByTarget.entries())
    );
    this.persistedStateByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      graph.warehouseState.cachedStates?.map(persistedTableMetadata => [
        persistedTableMetadata.target,
        persistedTableMetadata
      ])
    );
    this.nonTableDeclarationTargets = new StringifiedSet<dataform.ITarget>(
      JSONObjectStringifier.create(),
      graph.declarationTargets.filter(
        declarationTarget =>
          this.warehouseStateBeforeRunByTarget.get(declarationTarget)?.type !==
          dataform.TableMetadata.Type.TABLE
      )
    );

    const completedActionNames = new Set(
      this.runResult.actions
        .filter(action => action.status !== dataform.ActionResult.ExecutionStatus.RUNNING)
        .map(action => action.name)
    );
    this.pendingActions = graph.actions.filter(action => !completedActionNames.has(action.name));
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
      const now = Date.now();
      const runStartMillis = this.runResult.timing?.startTimeMillis?.toNumber?.() || now;
      const elapsedTimeMillis = now - runStartMillis;
      const timeoutMillis = this.graph.runConfig.timeoutMillis - elapsedTimeMillis;
      this.timeout = setTimeout(() => {
        this.timedOut = true;
        this.cancel();
      }, timeoutMillis);
    }
    return this;
  }

  public stop() {
    this.stopped = true;
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

  private notifyListeners() {
    return Promise.all(this.changeListeners.map(listener => listener(this.runResult)));
  }

  private async executeGraph() {
    const timer = Timer.start(this.runResult.timing);

    this.runResult.status = dataform.RunResult.ExecutionStatus.RUNNING;
    this.runResult.timing = timer.current();
    await this.notifyListeners();

    // If we're not resuming an existing run, prepare schemas.
    if (this.runResult.actions.length === 0) {
      await this.prepareAllSchemas();
    }

    // Recursively execute all actions as they become executable.
    await this.executeAllActionsReadyForExecution();

    if (this.stopped) {
      return this.runResult;
    }

    this.runResult.timing = timer.end();

    if (this.graph.runConfig && this.graph.runConfig.useRunCache) {
      await Promise.all(this.metadataReadPromises);
      await this.dbadapter.persistStateMetadata(
        this.graph.projectConfig.defaultDatabase,
        new StringifiedMap<
          dataform.ITarget,
          dataform.PersistedTableMetadata.ITransitiveInputMetadata
        >(
          JSONObjectStringifier.create<dataform.ITarget>(),
          Array.from(this.warehouseStateAfterRunByTarget.entries()).map(
            ([target, tableMetadata]) => [
              target,
              {
                target: tableMetadata.target,
                lastUpdatedMillis: tableMetadata.lastUpdatedMillis
              }
            ]
          )
        ),
        this.graph.actions,
        this.graph.actions.filter(executionAction => {
          if (executionAction.hermeticity !== dataform.ActionHermeticity.HERMETIC) {
            return false;
          }
          const executionActionResult = this.runResult.actions.find(
            actionResult => actionResult.name === executionAction.name
          );
          if (!executionActionResult) {
            return false;
          }
          if (
            ![
              dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
              dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
            ].includes(executionActionResult.status)
          ) {
            return false;
          }
          return true;
        }),
        {
          onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel)
        }
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
        if (!databaseSchemas.has(trueDatabase)) {
          databaseSchemas.set(trueDatabase, new Set<string>());
        }
        databaseSchemas.get(trueDatabase).add(target.schema);
      });

    if (this.graph.projectConfig.useRunCache) {
      if (!databaseSchemas.has(this.graph.projectConfig.defaultDatabase)) {
        databaseSchemas.set(this.graph.projectConfig.defaultDatabase, new Set<string>());
      }
      databaseSchemas
        .get(this.graph.projectConfig.defaultDatabase)
        .add(dbadapters.CACHED_STATE_TABLE_TARGET.schema);
    }

    // Create all nonexistent schemas.
    await Promise.all(
      Array.from(databaseSchemas.entries()).map(async ([database, schemas]) => {
        const existingSchemas = new Set(await this.dbadapter.schemas(database));
        await Promise.all(
          Array.from(schemas)
            .filter(schema => !existingSchemas.has(schema))
            .map(schema => this.dbadapter.createSchema(database, schema))
        );
      })
    );
  }

  private async executeAllActionsReadyForExecution() {
    if (this.stopped) {
      return;
    }

    // If the run has been cancelled, cancel all pending actions.
    if (this.cancelled) {
      const allPendingActions = this.pendingActions;
      this.pendingActions = [];
      allPendingActions.forEach(pendingAction =>
        this.runResult.actions.push({
          name: pendingAction.name,
          target: pendingAction.target,
          status: dataform.ActionResult.ExecutionStatus.SKIPPED,
          tasks: pendingAction.tasks.map(() => ({
            status: dataform.TaskResult.ExecutionStatus.SKIPPED
          }))
        })
      );
      await this.notifyListeners();
      return;
    }

    const executableActions = this.removeExecutableActionsFromPending();
    const skippableActions = this.removeSkippableActionsFromPending();

    await Promise.all([
      (async () => {
        skippableActions.forEach(skippableAction => {
          this.runResult.actions.push({
            name: skippableAction.name,
            target: skippableAction.target,
            status: dataform.ActionResult.ExecutionStatus.SKIPPED,
            tasks: skippableAction.tasks.map(() => ({
              status: dataform.TaskResult.ExecutionStatus.SKIPPED
            }))
          });
        });
        if (skippableActions.length > 0) {
          await this.notifyListeners();
          await this.executeAllActionsReadyForExecution();
        }
      })(),
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
      this.graph,
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
      this.graph,
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
        target: action.target,
        status: dataform.ActionResult.ExecutionStatus.DISABLED,
        tasks: []
      });
      await this.notifyListeners();
      return;
    }

    if (this.shouldCacheSkip(action)) {
      this.runResult.actions.push({
        name: action.name,
        target: action.target,
        status: dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
        tasks: []
      });
      await this.notifyListeners();
      return;
    }

    let actionResult = this.runResult.actions.find(
      existingActionResult => existingActionResult.name === action.name
    );
    const timer = Timer.start(actionResult?.timing);
    if (!actionResult) {
      actionResult = {
        name: action.name,
        target: action.target,
        status: dataform.ActionResult.ExecutionStatus.RUNNING,
        timing: timer.current(),
        tasks: []
      };
      this.runResult.actions.push(actionResult);
      await this.notifyListeners();
    }

    await this.dbadapter.withClientLock(async client => {
      // Start running tasks from the last executed task (if any), onwards.
      for (const task of action.tasks.slice(actionResult.tasks.length)) {
        if (this.stopped) {
          return;
        }
        if (
          actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
          !this.cancelled
        ) {
          const taskStatus = await this.executeTask(client, task, actionResult, {
            bigquery: { labels: action.actionDescriptor?.bigqueryLabels }
          });
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
    });

    if (this.stopped) {
      return;
    }

    if (actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
    }

    if (
      action.actionDescriptor &&
      actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL &&
      !(this.graph.runConfig && this.graph.runConfig.disableSetMetadata) &&
      action.type === "table" &&
      action.tableType !== "inline"
    ) {
      await this.dbadapter.setMetadata(action);
    }

    if (this.graph.projectConfig.useRunCache) {
      this.metadataReadPromises.push(
        (async () => {
          try {
            const newMetadata = await this.dbadapter.table(action.target);
            if (newMetadata) {
              this.warehouseStateAfterRunByTarget.set(action.target, newMetadata);
            } else {
              this.warehouseStateAfterRunByTarget.delete(action.target);
            }
          } catch (e) {
            // If something went wrong trying to get new table metadata, delete it.
            this.warehouseStateAfterRunByTarget.delete(action.target);
          }
        })()
      );
    }

    actionResult.timing = timer.end();
    await this.notifyListeners();
  }

  private async executeTask(
    client: dbadapters.IDbClient,
    task: dataform.IExecutionTask,
    parentAction: dataform.IActionResult,
    options: { bigquery: { labels: { [label: string]: string } } }
  ): Promise<dataform.TaskResult.ExecutionStatus> {
    const timer = Timer.start();
    const taskResult: dataform.ITaskResult = {
      status: dataform.TaskResult.ExecutionStatus.RUNNING,
      timing: timer.current(),
      metadata: {}
    };
    parentAction.tasks.push(taskResult);
    await this.notifyListeners();
    try {
      // Retry this function a given number of times, configurable by user
      const { rows, metadata } = await retry(
        () =>
          client.execute(task.statement, {
            onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel),
            rowLimit: 1,
            bigquery: options.bigquery
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
    await this.notifyListeners();
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

    // Persisted state for this action must exist, and the persisted action hash must match this action's hash.
    if (!this.persistedStateByTarget.has(executionAction.target)) {
      return false;
    }
    const persistedTableMetadata = this.persistedStateByTarget.get(executionAction.target);
    if (
      !timingSafeEqual(hashExecutionAction(executionAction), persistedTableMetadata.definitionHash)
    ) {
      return false;
    }

    // The target table for this action must exist, and the table metadata's last update timestamp must match
    // the persisted last update timestamp.
    if (!this.warehouseStateBeforeRunByTarget.has(executionAction.target)) {
      return false;
    }
    if (
      persistedTableMetadata.lastUpdatedMillis.notEquals(
        this.warehouseStateBeforeRunByTarget.get(executionAction.target).lastUpdatedMillis
      )
    ) {
      return false;
    }
    const persistedTransitiveInputUpdateTimestamps = new StringifiedMap(
      JSONObjectStringifier.create<dataform.ITarget>(),
      persistedTableMetadata.transitiveInputTables.map(transitiveInputTable => [
        transitiveInputTable.target,
        transitiveInputTable.lastUpdatedMillis
      ])
    );
    for (const transitiveInput of executionAction.transitiveInputs) {
      // No transitive input can be a non-table declaration (because we don't know anything about the
      // data upstream of that non-table).
      if (this.nonTableDeclarationTargets.has(transitiveInput)) {
        return false;
      }

      // All transitive inputs' last change timestamps must match the corresponding timestamps stored
      // in persisted state.
      if (!persistedTransitiveInputUpdateTimestamps.has(transitiveInput)) {
        return false;
      }
      const persistedTransitiveInputUpdateTimestamp = persistedTransitiveInputUpdateTimestamps.get(
        transitiveInput
      );
      if (!this.warehouseStateBeforeRunByTarget.has(transitiveInput)) {
        return false;
      }
      const latestTransitiveInputUpdateTimestamp = this.warehouseStateBeforeRunByTarget.get(
        transitiveInput
      ).lastUpdatedMillis;
      if (persistedTransitiveInputUpdateTimestamp.notEquals(latestTransitiveInputUpdateTimestamp)) {
        return false;
      }

      // All transitive inputs, if they were included in the run, must have completed with
      // CACHE_SKIPPED status.
      const transitiveInputAction = this.graph.actions.find(
        action =>
          action.target.database === transitiveInput.database &&
          action.target.schema === transitiveInput.schema &&
          action.target.name === transitiveInput.name
      );
      if (
        transitiveInputAction &&
        this.runResult.actions.find(action => action.name === transitiveInputAction.name).status !==
          dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
      ) {
        return false;
      }
    }

    return true;
  }
}

function allDependenciesHaveBeenExecuted(
  executionGraph: dataform.IExecutionGraph,
  actionResults: dataform.IActionResult[]
) {
  const allActionNames = new Set<string>(executionGraph.actions.map(action => action.name));
  const executedActionNames = new Set<string>(actionResults.map(action => action.name));
  return (action: dataform.IExecutionAction) => {
    for (const dependency of action.dependencies) {
      if (allActionNames.has(dependency) && !executedActionNames.has(dependency)) {
        return false;
      }
    }
    return true;
  };
}

class Timer {
  public static start(existingTiming?: dataform.ITiming) {
    return new Timer(existingTiming?.startTimeMillis.toNumber() || new Date().valueOf());
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
