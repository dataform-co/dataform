import EventEmitter from "events";
import Long from "long";

import * as dbadapters from "df/cli/api/dbadapters";
import { IBigQueryExecutionOptions } from "df/cli/api/dbadapters/bigquery";
import { Flags } from "df/common/flags";
import { retry } from "df/common/promises";
import { deepClone, equals } from "df/common/protos";
import { StringifiedMap, StringifiedSet } from "df/common/strings/stringifier";
import { IBigQueryOptions } from "df/core/actions/table";
import { targetStringifier } from "df/core/targets";
import { dataform } from "df/protos/ts";

const CANCEL_EVENT = "jobCancel";
const flags = {
  runnerNotificationPeriodMillis: Flags.number("runner-notification-period-millis", 5000)
};

const isSuccessfulAction = (actionResult: dataform.IActionResult) =>
  actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.DISABLED;

export interface IExecutedAction {
  executionAction: dataform.IExecutionAction;
  actionResult: dataform.IActionResult;
}

export interface IExecutionOptions {
  bigquery?: { jobPrefix?: string; actionRetryLimit?: number };
}

export function run(
  dbadapter: dbadapters.IDbAdapter,
  graph: dataform.IExecutionGraph,
  executionOptions?: IExecutionOptions,
  partiallyExecutedRunResult: dataform.IRunResult = {},
  runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get()
): Runner {
  return new Runner(
    dbadapter,
    graph,
    executionOptions,
    partiallyExecutedRunResult,
    runnerNotificationPeriodMillis
  ).execute();
}

export class Runner {
  private readonly warehouseStateByTarget: StringifiedMap<
    dataform.ITarget,
    dataform.ITableMetadata
  >;

  private readonly allActionTargets: StringifiedSet<dataform.ITarget>;
  private readonly runResult: dataform.IRunResult;
  private readonly changeListeners: Array<(graph: dataform.IRunResult) => void> = [];
  private readonly eEmitter: EventEmitter;
  private executedActionTargets: StringifiedSet<dataform.ITarget>;
  private successfullyExecutedActionTargets: StringifiedSet<dataform.ITarget>;
  private pendingActions: dataform.IExecutionAction[];
  private lastNotificationTimestampMillis = 0;
  private stopped = false;
  private cancelled = false;
  private timeout: NodeJS.Timer;
  private timedOut = false;
  private executionTask: Promise<dataform.IRunResult>;

  constructor(
    private readonly dbadapter: dbadapters.IDbAdapter,
    private readonly graph: dataform.IExecutionGraph,
    private readonly executionOptions: IExecutionOptions = {},
    partiallyExecutedRunResult: dataform.IRunResult = {},
    private readonly runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get()
  ) {
    this.allActionTargets = new StringifiedSet<dataform.ITarget>(
      targetStringifier,
      graph.actions.map(action => action.target)
    );
    this.runResult = {
      actions: [],
      ...partiallyExecutedRunResult
    };
    this.warehouseStateByTarget = new StringifiedMap(
      targetStringifier,
      graph.warehouseState.tables?.map(tableMetadata => [tableMetadata.target, tableMetadata])
    );
    this.executedActionTargets = new StringifiedSet(
      targetStringifier,
      this.runResult.actions
        .filter(action => action.status !== dataform.ActionResult.ExecutionStatus.RUNNING)
        .map(action => action.target)
    );
    this.successfullyExecutedActionTargets = new StringifiedSet(
      targetStringifier,
      this.runResult.actions.filter(isSuccessfulAction).map(action => action.target)
    );
    this.pendingActions = graph.actions.filter(
      action => !this.executedActionTargets.has(action.target)
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
    if (Date.now() - this.runnerNotificationPeriodMillis < this.lastNotificationTimestampMillis) {
      return;
    }
    const runResultClone = deepClone(dataform.RunResult, this.runResult);
    this.lastNotificationTimestampMillis = Date.now();
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
        pendingAction.dependencyTargets.every(
          dependency =>
            !this.allActionTargets.has(dependency) ||
            this.successfullyExecutedActionTargets.has(dependency)
        )
      ) {
        executableActions.push(pendingAction);
      } else if (
        // An action is skippable if it is not executable and all dependencies either: do not
        // exist in the graph, or have completed execution.
        pendingAction.dependencyTargets.every(
          dependency =>
            !this.allActionTargets.has(dependency) || this.executedActionTargets.has(dependency)
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
          this.executedActionTargets.add(executableAction.target);
          if (isSuccessfulAction(actionResult)) {
            this.successfullyExecutedActionTargets.add(executableAction.target);
          }
          await this.executeAllActionsReadyForExecution();
        })
      )
    ]);
  }

  private async executeAction(action: dataform.IExecutionAction): Promise<dataform.IActionResult> {
    let actionResult: dataform.IActionResult = {
      target: action.target,
      tasks: []
    };

    if (action.tasks.length === 0) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.DISABLED;
      this.runResult.actions.push(actionResult);
      this.notifyListeners();
      return actionResult;
    }

    const resumedActionResult = this.runResult.actions.find(existingActionResult =>
      equals(dataform.Target, existingActionResult.target, action.target)
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
            bigquery: {
              labels: action.actionDescriptor?.bigqueryLabels,
              actionRetryLimit: this.executionOptions?.bigquery?.actionRetryLimit,
              jobPrefix: this.executionOptions?.bigquery?.jobPrefix
            }
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
      action.type === "table"
    ) {
      try {
        await this.dbadapter.setMetadata(action);
      } catch (e) {
        // TODO: Setting the metadata is not a task itself, so we have nowhere to surface this error cleanly.
        // For now, we can attach the error to the last task in the action so it gets
        // surfaced properly without ending the entire run, but also not failing silently.
        if (actionResult.tasks.length > 0) {
          actionResult.tasks[
            actionResult.tasks.length - 1
          ].errorMessage = `Error setting metadata: ${e.message}`;
          actionResult.tasks[actionResult.tasks.length - 1].status =
            dataform.TaskResult.ExecutionStatus.FAILED;
        }
        actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
      }
    }

    this.warehouseStateByTarget.delete(action.target);

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
    options: { bigquery?: IBigQueryOptions & IBigQueryExecutionOptions }
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
        task.type === "operation" ? 1 : options.bigquery.actionRetryLimit + 1 || 1
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
