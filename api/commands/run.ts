import EventEmitter from "events";
import Long from "long";

import * as dbadapters from "df/api/dbadapters";
import { retry } from "df/common/promises";
import { deepClone, equals } from "df/common/protos";
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

export interface IExecutedAction {
  executionAction: dataform.IExecutionAction;
  actionResult: dataform.IActionResult;
}

export function run(
  dbadapter: dbadapters.IDbAdapter,
  graph: dataform.IExecutionGraph,
  partiallyExecutedRunResult: dataform.IRunResult = {},
  previouslyExecutedActions: IExecutedAction[] = []
): Runner {
  return new Runner(
    dbadapter,
    graph,
    partiallyExecutedRunResult,
    previouslyExecutedActions
  ).execute();
}

export class Runner {
  private readonly warehouseStateByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.ITableMetadata
  >;
  private readonly nonTableDeclarationTargets: StringifiedSet<dataform.ITarget>;

  private readonly previouslyExecutedActions: StringifiedMap<dataform.ITarget, IExecutedAction>;

  private readonly allActionNames: Set<string>;
  private readonly runResult: dataform.IRunResult;
  private readonly changeListeners: Array<(graph: dataform.IRunResult) => void> = [];
  private readonly eEmitter: EventEmitter;

  private executedActionNames: Set<string>;
  private successfullyExecutedActionNames: Set<string>;
  private pendingActions: dataform.IExecutionAction[];
  private stopped = false;
  private cancelled = false;
  private timeout: NodeJS.Timer;
  private timedOut = false;
  private executionTask: Promise<dataform.IRunResult>;

  constructor(
    private readonly dbadapter: dbadapters.IDbAdapter,
    private readonly graph: dataform.IExecutionGraph,
    partiallyExecutedRunResult: dataform.IRunResult = {},
    previouslyExecutedActions: IExecutedAction[] = []
  ) {
    this.allActionNames = new Set<string>(graph.actions.map(action => action.name));
    this.runResult = {
      actions: [],
      ...partiallyExecutedRunResult
    };
    this.warehouseStateByTarget = new StringifiedMap(
      JSONObjectStringifier.create(),
      graph.warehouseState.tables?.map(tableMetadata => [tableMetadata.target, tableMetadata])
    );
    this.nonTableDeclarationTargets = new StringifiedSet<dataform.ITarget>(
      JSONObjectStringifier.create(),
      graph.declarationTargets.filter(
        declarationTarget =>
          this.warehouseStateByTarget.get(declarationTarget)?.type !==
          dataform.TableMetadata.Type.TABLE
      )
    );

    this.previouslyExecutedActions = new StringifiedMap(
      JSONObjectStringifier.create(),
      previouslyExecutedActions.map(executedAction => [
        executedAction.executionAction.target,
        executedAction
      ])
    );

    this.executedActionNames = new Set(
      this.runResult.actions
        .filter(action => action.status !== dataform.ActionResult.ExecutionStatus.RUNNING)
        .map(action => action.name)
    );
    this.successfullyExecutedActionNames = new Set(
      this.runResult.actions.filter(isSuccessfulAction).map(action => action.name)
    );
    this.pendingActions = graph.actions.filter(
      action => !this.executedActionNames.has(action.name)
    );
    this.eEmitter = new EventEmitter();
    // There could feasibly be thousands of listeners to this, 0 makes the limit infinite.
    this.eEmitter.setMaxListeners(0);
  }

  public onChange(listener: (graph: dataform.IRunResult) => void): Runner {
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
    const runResultClone = deepClone(dataform.RunResult, this.runResult);
    this.changeListeners.forEach(listener => listener(runResultClone));
  }

  private async executeGraph() {
    const timer = Timer.start(this.runResult.timing);

    this.runResult.status = dataform.RunResult.ExecutionStatus.RUNNING;
    this.runResult.timing = timer.current();
    this.notifyListeners();

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
      this.notifyListeners();
      return;
    }

    const executableActions = [];
    const skippableActions = [];
    const stillPendingActions = [];
    for (const pendingAction of this.pendingActions) {
      if (
        // An action is executable if all dependencies either: do not exist in the graph, or
        // have executed successfully.
        pendingAction.dependencies.every(
          dependency =>
            !this.allActionNames.has(dependency) ||
            this.successfullyExecutedActionNames.has(dependency)
        )
      ) {
        executableActions.push(pendingAction);
      } else if (
        // An action is skippable if it is not executable and all dependencies either: do not
        // exist in the graph, or have completed execution.
        pendingAction.dependencies.every(
          dependency =>
            !this.allActionNames.has(dependency) || this.executedActionNames.has(dependency)
        )
      ) {
        skippableActions.push(pendingAction);
      } else {
        // Otherwise, the action is still pending.
        stillPendingActions.push(pendingAction);
      }
    }
    this.pendingActions = stillPendingActions;

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
          this.notifyListeners();
          await this.executeAllActionsReadyForExecution();
        }
      })(),
      Promise.all(
        executableActions.map(async executableAction => {
          const actionResult = await this.executeAction(executableAction);
          this.executedActionNames.add(executableAction.name);
          if (isSuccessfulAction(actionResult)) {
            this.successfullyExecutedActionNames.add(executableAction.name);
          }
          await this.executeAllActionsReadyForExecution();
        })
      )
    ]);
  }

  private async executeAction(action: dataform.IExecutionAction): Promise<dataform.IActionResult> {
    let actionResult: dataform.IActionResult = {
      name: action.name,
      target: action.target,
      tasks: [],
      inputs: action.transitiveInputs.map(target => ({
        target,
        metadata: this.warehouseStateByTarget.has(target)
          ? {
              lastModifiedTimestampMillis: this.warehouseStateByTarget.get(target).lastUpdatedMillis
            }
          : null
      }))
    };

    if (action.tasks.length === 0) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.DISABLED;
      this.runResult.actions.push(actionResult);
      this.notifyListeners();
      return actionResult;
    }

    if (this.shouldCacheSkip(action)) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED;
      this.runResult.actions.push(actionResult);
      this.notifyListeners();
      return actionResult;
    }

    const resumedActionResult = this.runResult.actions.find(
      existingActionResult => existingActionResult.name === action.name
    );
    if (resumedActionResult) {
      actionResult = resumedActionResult;
    } else {
      this.runResult.actions.push(actionResult);
    }
    actionResult.status = dataform.ActionResult.ExecutionStatus.RUNNING;
    const timer = Timer.start(resumedActionResult?.timing);
    actionResult.timing = timer.current();
    this.notifyListeners();

    await this.dbadapter.withClientLock(async client => {
      // Start running tasks from the last executed task (if any), onwards.
      for (const task of action.tasks.slice(actionResult.tasks.length)) {
        if (this.stopped) {
          return actionResult;
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
      return actionResult;
    }

    if (
      action.actionDescriptor &&
      // Only set metadata if we expect the action to complete in SUCCESSFUL state
      // (i.e. it must still be RUNNING, and not FAILED).
      actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
      !(this.graph.runConfig && this.graph.runConfig.disableSetMetadata) &&
      action.type === "table" &&
      action.tableType !== "inline"
    ) {
      await this.dbadapter.setMetadata(action);
    }

    let newMetadata: dataform.ITableMetadata;
    if (this.graph.projectConfig.useRunCache) {
      try {
        newMetadata = await this.dbadapter.table(action.target);
      } catch (e) {
        // Ignore Errors thrown when trying to get new table metadata; just allow the relevant
        // warehouseStateAfterRunByTarget entry to be cleared out (below).
      }
    }
    if (newMetadata) {
      this.warehouseStateByTarget.set(action.target, newMetadata);
      actionResult.postExecutionTimestampMillis = newMetadata.lastUpdatedMillis;
      this.notifyListeners();
    } else {
      this.warehouseStateByTarget.delete(action.target);
    }

    if (actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
    }

    actionResult.timing = timer.end();
    this.notifyListeners();
    return actionResult;
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
    this.notifyListeners();
    try {
      // Retry this function a given number of times, configurable by user
      const { rows, metadata } = await retry(
        () =>
          client.execute(task.statement, {
            onCancel: handleCancel => this.eEmitter.on(CANCEL_EVENT, handleCancel),
            rowLimit: 1,
            bigquery: options.bigquery
          }),
        task.type === "operation" ? 1 : this.graph.projectConfig.idempotentActionRetries + 1 || 1
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
    this.notifyListeners();
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

    // This action must have been executed successfully before, and the previous ExecutionAction
    // must be equal to this one.
    if (!this.previouslyExecutedActions.has(executionAction.target)) {
      return false;
    }
    const previouslyExecutedAction = this.previouslyExecutedActions.get(executionAction.target);
    if (
      previouslyExecutedAction.actionResult.status !==
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    ) {
      return false;
    }
    if (
      !equals(dataform.ExecutionAction, previouslyExecutedAction.executionAction, executionAction)
    ) {
      return false;
    }

    // The target table for this action must exist, and the table metadata's last update timestamp must match
    // the timestamp recorded after the most recent execution.
    if (!this.warehouseStateByTarget.has(executionAction.target)) {
      return false;
    }
    if (
      this.warehouseStateByTarget.get(executionAction.target).lastUpdatedMillis.equals(0) ||
      previouslyExecutedAction.actionResult.postExecutionTimestampMillis.equals(0) ||
      this.warehouseStateByTarget
        .get(executionAction.target)
        .lastUpdatedMillis.notEquals(
          previouslyExecutedAction.actionResult.postExecutionTimestampMillis
        )
    ) {
      return false;
    }

    const previousInputTimestamps = new StringifiedMap(
      JSONObjectStringifier.create<dataform.ITarget>(),
      previouslyExecutedAction.actionResult.inputs
        .filter(input => !!input.metadata)
        .map(input => [input.target, input.metadata.lastModifiedTimestampMillis])
    );
    for (const transitiveInput of executionAction.transitiveInputs) {
      // No transitive input can be a non-table declaration (because we don't know anything about the
      // data upstream of that non-table).
      if (this.nonTableDeclarationTargets.has(transitiveInput)) {
        return false;
      }

      // All transitive inputs' last change timestamps must match the corresponding timestamps stored
      // in persisted state.
      if (!previousInputTimestamps.has(transitiveInput)) {
        return false;
      }
      if (!this.warehouseStateByTarget.has(transitiveInput)) {
        return false;
      }
      const inputWarehouseState = this.warehouseStateByTarget.get(transitiveInput);
      if (
        this.warehouseStateByTarget.get(transitiveInput).lastUpdatedMillis.equals(0) ||
        previousInputTimestamps.get(transitiveInput).equals(0) ||
        inputWarehouseState.lastUpdatedMillis.notEquals(
          previousInputTimestamps.get(transitiveInput)
        ) ||
        // If the input has a streaming buffer, we cannot trust its last-updated timestamp.
        inputWarehouseState.bigquery?.hasStreamingBuffer
      ) {
        return false;
      }
    }

    return true;
  }
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
