import EventEmitter from "events";
import Long from "long";

import { JitCompileChildProcess, JitCompiler } from "df/cli/api/commands/jit/compiler";
import * as dbadapters from "df/cli/api/dbadapters";
import { IBigQueryExecutionOptions } from "df/cli/api/dbadapters/bigquery";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { concatenateQueries, wrapWithCreateSchemaIfNotExists } from "df/cli/api/dbadapters/tasks";
import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { Flags } from "df/common/flags";
import { retry } from "df/common/promises";
import { deepClone, equals } from "df/common/protos";
import { targetStringifier } from "df/core/targets";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

const CANCEL_EVENT = "jobCancel";
const flags = {
  runnerNotificationPeriodMillis: Flags.number("runner-notification-period-millis", 5000),
};

const isSuccessfulAction = (actionResult: dataform.IActionResult) =>
  actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL ||
  actionResult.status === dataform.ActionResult.ExecutionStatus.DISABLED;

export interface IExecutedAction {
  executionAction: dataform.IExecutionAction;
  actionResult: dataform.IActionResult;
}

export interface IExecutionOptions {
  /**
   * Directory JiT compilation runs in; `@dataform/core` is loaded from its `node_modules`. Use the
   * compilation environment's directory (see `compileForRun`) so JiT uses the same core.
   */
  projectDir?: string;
  bigquery?: {
    jobPrefix?: string;
    actionRetryLimit?: number;
    dryRun?: boolean;
    labels?: { [label: string]: string };
  };
  jitCompiler?: JitCompiler;
  /**
   * The `@dataform/core` version that compiled the project (`CompiledGraph.dataformCoreVersion`).
   * JiT compilation must use the same version.
   */
  expectedCoreVersion?: string;
  lineageEmitter?: LineageEmitter;
  dryRunValidationQueries?: Map<string, string>;
}

/**
 * Key of `IExecutionOptions.dryRunValidationQueries`. Unlike `targetStringifier`, it doesn't depend
 * on which (possibly empty) fields happen to be set on the target proto, so that targets of the
 * compiled graph and of the execution graph match.
 */
export function dryRunValidationQueryKey(target: dataform.ITarget): string {
  return JSON.stringify([target.database || "", target.schema || "", target.name || ""]);
}

/**
 * Returns the compiled SELECT queries (including pre and post operations) of the tables, views,
 * incremental tables and assertions of the graph, keyed by `dryRunValidationQueryKey()`, for
 * `IExecutionOptions.dryRunValidationQueries`. JiT actions are skipped: their query is only known
 * once they're compiled during the run.
 */
export function buildDryRunValidationQueries(
  compiledGraph: dataform.ICompiledGraph,
): Map<string, string> {
  const queries = new Map<string, string>();
  [...(compiledGraph.tables || []), ...(compiledGraph.assertions || [])]
    .filter((action) => !action.disabled)
    .forEach((action) => {
      const query = actionDryRunValidationQuery(action);
      if (query) {
        queries.set(dryRunValidationQueryKey(action.target), query);
      }
    });
  return queries;
}

function actionDryRunValidationQuery(
  action?: dataform.ITable | null,
): string | undefined {
  if (!action?.query?.trim()) {
    return undefined;
  }
  return concatenateQueries([...(action.preOps || []), action.query, ...(action.postOps || [])]);
}

export type CancelReason = "timeout" | "user" | "cancellation";

/** A target dataset that doesn't exist yet. */
interface IMissingSchema {
  // Unset if the action's target has no database and there's no default database, in which case
  // BigQuery uses the project of the credentials.
  database: string | undefined;
  schema: string;
}

export function run(
  dbadapter: dbadapters.IDbAdapter,
  graph: dataform.IExecutionGraph,
  executionOptions: IExecutionOptions = {},
  partiallyExecutedRunResult: dataform.IRunResult = {},
  runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get(),
): Runner {
  return new Runner(
    dbadapter,
    graph,
    executionOptions,
    partiallyExecutedRunResult,
    runnerNotificationPeriodMillis,
  ).execute();
}

export class Runner {
  public static create(
    dbadapter: dbadapters.IDbAdapter,
    graph: dataform.IExecutionGraph,
    options: IExecutionOptions = {},
    runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get(),
  ): Runner {
    return new Runner(dbadapter, graph, options, {}, runnerNotificationPeriodMillis);
  }

  public static resume(
    dbadapter: dbadapters.IDbAdapter,
    graph: dataform.IExecutionGraph,
    runResult: dataform.IRunResult,
    runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get(),
  ): Runner {
    return new Runner(dbadapter, graph, {}, runResult, runnerNotificationPeriodMillis);
  }

  private readonly warehouseStateByTarget: Map<string, dataform.ITableMetadata>;
  private readonly executionSql: ExecutionSql;
  private readonly executionOptions: IExecutionOptions;

  private readonly allActionTargets: Set<string>;
  private readonly runResult: dataform.IRunResult;
  private readonly changeListeners: Array<(graph: dataform.IRunResult) => void> = [];
  private readonly eEmitter: EventEmitter;
  private executedActionTargets: Set<string>;
  private successfullyExecutedActionTargets: Set<string>;
  private pendingActions: dataform.IExecutionAction[];
  private lastNotificationTimestampMillis = 0;
  private stopped = false;
  private cancelled = false;
  private timeout: NodeJS.Timer;
  private timedOut = false;
  private skipReason: string = "";
  private executionTask: Promise<dataform.IRunResult>;
  private missingSchemas = new Map<string, IMissingSchema>();
  private schemaCreations = new Map<string, Promise<void>>();
  private jitDryRunValidationQueries = new Map<string, string>();

  constructor(
    private readonly dbadapter: dbadapters.IDbAdapter,
    private readonly graph: dataform.IExecutionGraph,
    executionOptions: IExecutionOptions = {},
    partiallyExecutedRunResult: dataform.IRunResult = {},
    private readonly runnerNotificationPeriodMillis: number = flags.runnerNotificationPeriodMillis.get(),
  ) {
    this.executionOptions = { projectDir: ".", ...executionOptions };
    this.runResult = {
      actions: [],
      ...partiallyExecutedRunResult,
    };

    if (!this.executionOptions.jitCompiler) {
      this.executionOptions.jitCompiler = JitCompileChildProcess.compile;
    }
    this.executionSql = new ExecutionSql(graph.projectConfig, version);
    this.allActionTargets = new Set<string>(
      graph.actions.map((action) => targetStringifier.stringify(action.target)),
    );
    this.warehouseStateByTarget = new Map<string, dataform.ITableMetadata>();
    graph.warehouseState.tables?.forEach((tableMetadata) =>
      this.warehouseStateByTarget.set(
        targetStringifier.stringify(tableMetadata.target),
        tableMetadata,
      ),
    );
    this.executedActionTargets = new Set(
      this.runResult.actions
        .filter((action) => action.status !== dataform.ActionResult.ExecutionStatus.RUNNING)
        .map((action) => targetStringifier.stringify(action.target)),
    );
    this.successfullyExecutedActionTargets = new Set<string>(
      this.runResult.actions
        .filter(isSuccessfulAction)
        .map((action) => targetStringifier.stringify(action.target)),
    );
    this.pendingActions = graph.actions.filter(
      (action) => !this.executedActionTargets.has(targetStringifier.stringify(action.target)),
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
    if (this.executionTask !== undefined) {
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
        this.cancel("timeout");
      }, timeoutMillis);
    }
    return this;
  }

  public stop() {
    this.stopped = true;
  }

  public cancel(reason: CancelReason = "user") {
    if (!this.skipReason) {
      switch (reason) {
        case "timeout":
          this.skipReason = this.graph.runConfig?.timeoutMillis
            ? `Run timed out after ${this.graph.runConfig.timeoutMillis / 1000} seconds`
            : "Run timed out";
          break;
        case "user":
          this.skipReason = "Run cancelled";
          break;
        case "cancellation":
          this.skipReason = "Run cancelled before action started";
          break;
      }
    }
    this.cancelled = true;
    this.eEmitter.emit(CANCEL_EVENT, undefined, undefined);
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
    this.changeListeners.forEach((listener) => listener(runResultClone));
  }

  private async executeGraph() {
    const timer = Timer.start(this.runResult.timing);

    this.runResult.status = dataform.RunResult.ExecutionStatus.RUNNING;
    this.runResult.timing = timer.current();
    this.notifyListeners();

    this.missingSchemas = await this.findMissingSchemas();

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
        (action) => action.status === dataform.ActionResult.ExecutionStatus.FAILED,
      )
    ) {
      this.runResult.status = dataform.RunResult.ExecutionStatus.FAILED;
    }

    return this.runResult;
  }

  /**
   * Returns the target datasets of all actions that don't exist yet, keyed by `schemaKey()`.
   */
  private async findMissingSchemas(): Promise<Map<string, IMissingSchema>> {
    // Work out all the schemas we are going to need.
    const databaseSchemas = new Map<string | undefined, Set<string>>();
    this.graph.actions
      .filter((action) => !!action.target && !!action.target.schema)
      .forEach(({ target }) => {
        const trueDatabase = this.targetDatabase(target!);
        if (!databaseSchemas.has(trueDatabase)) {
          databaseSchemas.set(trueDatabase, new Set<string>());
        }
        databaseSchemas.get(trueDatabase).add(target.schema);
      });

    // Find all nonexistent schemas.
    const missingSchemas = new Map<string, IMissingSchema>();
    await Promise.all(
      Array.from(databaseSchemas.entries()).map(async ([database, schemas]) => {
        const existingSchemas = new Set(await this.dbadapter.schemas(database));
        Array.from(schemas)
          .filter((schema) => !existingSchemas.has(schema))
          .forEach((schema) =>
            missingSchemas.set(this.schemaKey({ database, schema }), { database, schema }),
          );
      }),
    );
    return missingSchemas;
  }

  private targetDatabase(target: dataform.ITarget): string | undefined {
    // Either may be unset (e.g. no defaultProject in workflow_settings.yaml, or older versions of
    // dataform), in which case BigQuery uses the project of the credentials.
    return target.database || this.graph.projectConfig!.defaultDatabase || undefined;
  }

  private schemaKey(target: dataform.ITarget): string {
    return `${this.targetDatabase(target)}.${target.schema}`;
  }

  private async executeAllActionsReadyForExecution() {
    if (this.stopped) {
      return;
    }

    // If the run has been cancelled, cancel all pending actions.
    if (this.cancelled) {
      const allPendingActions = this.pendingActions;
      this.pendingActions = [];
      const skipErrorMessage = this.skipReason || "Run cancelled before action started";
      allPendingActions.forEach((pendingAction) => {
        let tasks: dataform.ITaskResult[];
        if (pendingAction.tasks && pendingAction.tasks.length > 0) {
          tasks = pendingAction.tasks.map((_, i) => ({
            status: dataform.TaskResult.ExecutionStatus.SKIPPED,
            ...(i === 0 ? { errorMessage: skipErrorMessage } : {}),
          }));
        } else {
          tasks = [
            {
              status: dataform.TaskResult.ExecutionStatus.SKIPPED,
              errorMessage: skipErrorMessage,
            },
          ];
        }
        this.runResult.actions.push({
          target: pendingAction.target,
          status: dataform.ActionResult.ExecutionStatus.SKIPPED,
          tasks,
        });
      });
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
          (dependency) =>
            !this.allActionTargets.has(targetStringifier.stringify(dependency)) ||
            this.successfullyExecutedActionTargets.has(targetStringifier.stringify(dependency)),
        )
      ) {
        executableActions.push(pendingAction);
      } else if (
        // An action is skippable if it is not executable and all dependencies either: do not
        // exist in the graph, or have completed execution.
        pendingAction.dependencyTargets.every(
          (dependency) =>
            !this.allActionTargets.has(targetStringifier.stringify(dependency)) ||
            this.executedActionTargets.has(targetStringifier.stringify(dependency)),
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
        skippableActions.forEach((skippableAction) => {
          this.runResult.actions.push({
            target: skippableAction.target,
            status: dataform.ActionResult.ExecutionStatus.SKIPPED,
            tasks: skippableAction.tasks.map(() => ({
              status: dataform.TaskResult.ExecutionStatus.SKIPPED,
            })),
          });
        });
        if (skippableActions.length > 0) {
          this.notifyListeners();
          await this.executeAllActionsReadyForExecution();
        }
      })(),
      Promise.all(
        executableActions.map(async (executableAction) => {
          const actionResult = await this.executeAction(executableAction);
          this.executedActionTargets.add(targetStringifier.stringify(executableAction.target));
          if (isSuccessfulAction(actionResult)) {
            this.successfullyExecutedActionTargets.add(
              targetStringifier.stringify(executableAction.target),
            );
          }
          await this.executeAllActionsReadyForExecution();
        }),
      ),
    ]);
  }

  private getBigQueryExecutionOptions(
    action: dataform.IExecutionAction,
  ): IBigQueryExecutionOptions {
    return {
      dryRun: this.executionOptions.bigquery?.dryRun,
      jobPrefix: this.executionOptions.bigquery?.jobPrefix,
      labels: {
        ...(this.executionOptions?.bigquery?.labels || {}),
        ...(action.actionDescriptor?.bigqueryLabels || {}),
      },
      actionRetryLimit: this.executionOptions.bigquery?.actionRetryLimit,
      location: this.graph.projectConfig?.defaultLocation || undefined,
      reservation:
        action.actionDescriptor?.reservation || this.graph.projectConfig?.defaultReservation,
    };
  }

  private async executeAction(action: dataform.IExecutionAction): Promise<dataform.IActionResult> {
    let actionResult: dataform.IActionResult = {
      target: action.target,
      tasks: [],
    };

    if (action.tasks.length === 0 && !action.jitCode) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.DISABLED;
      this.runResult.actions.push(actionResult);
      this.notifyListeners();
      return actionResult;
    }

    const resumedActionResult = this.runResult.actions.find((existingActionResult) =>
      equals(dataform.Target, existingActionResult.target, action.target),
    );
    if (resumedActionResult) {
      actionResult = resumedActionResult;
    } else {
      this.runResult.actions.push(actionResult);
    }
    actionResult.status = dataform.ActionResult.ExecutionStatus.RUNNING;
    const timer = Timer.start(resumedActionResult?.timing);
    actionResult.timing = timer.current();
    if (this.executionOptions.lineageEmitter) {
      this.executionOptions.lineageEmitter.emitForAction(action, actionResult);
    }
    this.notifyListeners();

    if (action.jitCode) {
      try {
        await this.compileJitAction(action, this.dbadapter);
      } catch (e) {
        actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
        actionResult.tasks.push({
          status: dataform.TaskResult.ExecutionStatus.FAILED,
          errorMessage: `JiT compilation error: ${e?.message || String(e)}`,
        });
        actionResult.timing = timer.end();
        if (this.executionOptions.lineageEmitter) {
          this.executionOptions.lineageEmitter.emitForAction(action, actionResult);
        }
        this.notifyListeners();
        return actionResult;
      }
    }

    try {
      // Start running tasks from the last executed task (if any), onwards.
      for (const task of action.tasks.slice(actionResult.tasks.length)) {
        if (
          actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
          !this.stopped &&
          !this.cancelled
        ) {
          let schemaCreation = this.schemaCreationToWaitFor(action, task);
          while (schemaCreation !== undefined && !this.stopped && !this.cancelled) {
            await schemaCreation;
            schemaCreation = this.schemaCreationToWaitFor(action, task);
          }
        }
        if (this.stopped) {
          return actionResult;
        }
        if (
          actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
          !this.cancelled
        ) {
          const taskStatus = await this.executeTaskCreatingMissingSchema(
            action,
            task,
            actionResult,
          );
          if (taskStatus === dataform.TaskResult.ExecutionStatus.FAILED) {
            actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
          } else if (taskStatus === dataform.TaskResult.ExecutionStatus.CANCELLED) {
            actionResult.status = dataform.ActionResult.ExecutionStatus.CANCELLED;
          }
        } else {
          actionResult.tasks.push({
            status: dataform.TaskResult.ExecutionStatus.SKIPPED,
            errorMessage: this.skipReason || "Task skipped after cancellation",
          });
        }
      }
    } catch (e) {
      if (
        (actionResult.status as dataform.ActionResult.ExecutionStatus) !==
        dataform.ActionResult.ExecutionStatus.FAILED
      ) {
        actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
        actionResult.tasks.push({
          status: dataform.TaskResult.ExecutionStatus.FAILED,
          errorMessage: `Unexpected execution error: ${e.message}`,
        });
      }
    }

    if (this.stopped) {
      return actionResult;
    }

    if (
      action.actionDescriptor &&
      // Only set metadata if we expect the action to complete in SUCCESSFUL state
      // (i.e. it must still be RUNNING, and not FAILED).
      actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING &&
      !(this.graph.runConfig && this.graph.runConfig.disableSetMetadata) &&
      // Only set metadata if not using BigQuery dry run
      !this.executionOptions.bigquery?.dryRun &&
      action.type === "table"
    ) {
      try {
        await this.dbadapter.setMetadata(action);
      } catch (e) {
        // TODO: Setting the metadata is not a task itself, so we have nowhere to surface this error cleanly.
        // For now, we can attach the error to the last task in the action so it gets
        // surfaced properly without ending the entire run, but also not failing silently.
        if (actionResult.tasks.length > 0) {
          actionResult.tasks[actionResult.tasks.length - 1].errorMessage =
            `Error setting metadata: ${e.message}`;
          actionResult.tasks[actionResult.tasks.length - 1].status =
            dataform.TaskResult.ExecutionStatus.FAILED;
        }
        actionResult.status = dataform.ActionResult.ExecutionStatus.FAILED;
      }
    }

    this.warehouseStateByTarget.delete(targetStringifier.stringify(action.target));

    if (actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING) {
      actionResult.status = dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
    }

    actionResult.timing = timer.end();
    if (this.executionOptions.lineageEmitter) {
      this.executionOptions.lineageEmitter.emitForAction(action, actionResult);
    }
    this.notifyListeners();
    return actionResult;
  }

  /**
   * Returns the target dataset to create in the same job as the given task, or null if there's
   * none. Every task in an action is checked.
   */
  private missingSchemaToCreate(
    action: dataform.IExecutionAction,
    task: dataform.IExecutionTask,
  ): IMissingSchema | null {
    if (!action.target?.schema) {
      return null;
    }
    // Assertion tasks must stay single statements: their result is read from the job's rows,
    // and a script's result would be that of its last statement.
    if (task.type === "assertion") {
      return null;
    }
    return this.missingSchemas.get(this.schemaKey(action.target)) || null;
  }

  /**
   * Returns the job currently trying to create the given task's target dataset, if the task would
   * otherwise try to create it too. When it finishes, check again: another waiting task may have
   * become the next creator in the meantime.
   */
  private schemaCreationToWaitFor(
    action: dataform.IExecutionAction,
    task: dataform.IExecutionTask,
  ): Promise<void> | undefined {
    if (!this.missingSchemaToCreate(action, task)) {
      return undefined;
    }
    return this.schemaCreations.get(this.schemaKey(action.target));
  }

  /**
   * Executes the task. If its target dataset is still missing, the task is wrapped so that the
   * dataset is created in the same job. In a real run (not a dry run), other tasks writing to the
   * dataset wait for that job (see `schemaCreationToWaitFor()`): if it succeeds, they run unwrapped;
   * if it fails, the dataset may or may not have been created, so the next one tries to create it.
   */
  private async executeTaskCreatingMissingSchema(
    action: dataform.IExecutionAction,
    task: dataform.IExecutionTask,
    actionResult: dataform.IActionResult,
  ): Promise<dataform.TaskResult.ExecutionStatus> {
    const options = { bigquery: this.getBigQueryExecutionOptions(action) };
    const missingSchema = this.missingSchemaToCreate(action, task);
    if (!missingSchema) {
      return this.executeTask(this.dbadapter, task, actionResult, options);
    }
    const statement = wrapWithCreateSchemaIfNotExists(
      missingSchema.database,
      missingSchema.schema,
      task.statement,
    );
    // Dry runs don't create the dataset, so there's no rate limit to protect, and nothing to wait
    // for: every task writing to the dataset is wrapped.
    if (this.executionOptions.bigquery?.dryRun) {
      const status = await this.executeTask(this.dbadapter, task, actionResult, options, statement);
      if (status !== dataform.TaskResult.ExecutionStatus.SUCCESSFUL) {
        return status;
      }
      return this.dryRunValidationQuery(action, actionResult, missingSchema, options);
    }

    // Registered without awaiting anything since `schemaCreationToWaitFor()` returned nothing, so
    // no other task can become the creator in between.
    const key = this.schemaKey(action.target);
    let release: () => void;
    this.schemaCreations.set(key, new Promise<void>((resolve) => (release = resolve)));
    try {
      const status = await this.executeTask(this.dbadapter, task, actionResult, options, statement);
      if (status === dataform.TaskResult.ExecutionStatus.SUCCESSFUL) {
        this.missingSchemas.delete(key);
      }
      return status;
    } finally {
      this.schemaCreations.delete(key);
      release!();
    }
  }

  /**
   * In a dry run, BigQuery only checks the syntax of the wrapped script of a task writing to a
   * dataset that doesn't exist yet. Dry-run the action's SELECT query on its own, if it has one, to
   * fully check it. A failure is reported on the task, which was just executed.
   */
  private async dryRunValidationQuery(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult,
    missingSchema: IMissingSchema,
    options: { bigquery?: dataform.IBigQueryOptions & IBigQueryExecutionOptions },
  ): Promise<dataform.TaskResult.ExecutionStatus> {
    const key = dryRunValidationQueryKey(action.target);
    const query =
      this.jitDryRunValidationQueries.get(key) ??
      this.executionOptions.dryRunValidationQueries?.get(key);
    if (!query) {
      return dataform.TaskResult.ExecutionStatus.SUCCESSFUL;
    }
    try {
      await this.dbadapter.execute(query, {
        onCancel: (handleCancel) => this.eEmitter.on(CANCEL_EVENT, handleCancel),
        rowLimit: 1,
        bigquery: options.bigquery,
      });
      return dataform.TaskResult.ExecutionStatus.SUCCESSFUL;
    } catch (e) {
      const taskResult = actionResult.tasks[actionResult.tasks.length - 1];
      taskResult.status = this.cancelled
        ? dataform.TaskResult.ExecutionStatus.CANCELLED
        : dataform.TaskResult.ExecutionStatus.FAILED;
      const schemaRef = [missingSchema.database, missingSchema.schema].filter(Boolean).join(".");
      taskResult.errorMessage =
        `${this.graph.projectConfig!.warehouse} error: ${e.message} (in the query, validated ` +
        `separately because dataset ${schemaRef} doesn't exist yet)`;
      this.notifyListeners();
      return taskResult.status;
    }
  }

  /**
   * Executes the given task. If set, `statementOverride` is executed instead of the task's
   * statement, while the task's own statement is still reported as its compiled SQL.
   */
  private async executeTask(
    client: dbadapters.IDbClient,
    task: dataform.IExecutionTask,
    parentAction: dataform.IActionResult,
    options: { bigquery?: dataform.IBigQueryOptions & IBigQueryExecutionOptions },
    statementOverride?: string,
  ): Promise<dataform.TaskResult.ExecutionStatus> {
    const timer = Timer.start();
    const taskResult: dataform.ITaskResult = {
      status: dataform.TaskResult.ExecutionStatus.RUNNING,
      timing: timer.current(),
      metadata: {},
    };
    parentAction.tasks.push(taskResult);
    this.notifyListeners();
    if (options.bigquery?.dryRun) {
      taskResult.compiledSql = task.statement;
    }
    if (options.bigquery?.dryRun && task.type === "assertion") {
      taskResult.status = dataform.TaskResult.ExecutionStatus.SUCCESSFUL;
    } else {
      try {
        // Retry this function a given number of times, configurable by user
        const { rows, metadata } = await retry(
          () =>
            client.execute(statementOverride ?? task.statement, {
              onCancel: (handleCancel) => this.eEmitter.on(CANCEL_EVENT, handleCancel),
              rowLimit: 1,
              bigquery: options.bigquery,
            }),
          task.type === "operation" ? 1 : options.bigquery.actionRetryLimit + 1 || 1,
        );
        taskResult.metadata = metadata;
        if (task.type === "assertion") {
          // We expect that an assertion query returns 1 row, with 1 field that is the row count.
          // We don't really care what that field/column is called.
          const rowCount = rows[0]?.[Object.keys(rows[0])[0]];
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
        if (e.metadata?.bigquery?.jobId) {
          taskResult.metadata = {
            bigquery: {
              jobId: e.metadata.bigquery.jobId,
            },
          };
        }
      }
    }
    taskResult.timing = timer.end();
    this.notifyListeners();
    return taskResult.status;
  }

  private async compileJitAction(action: dataform.IExecutionAction, client: dbadapters.IDbClient) {
    let compilationTargetType =
      dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_UNSPECIFIED;
    if (action.type === "table") {
      compilationTargetType =
        action.tableType === "incremental"
          ? dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_INCREMENTAL_TABLE
          : dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE;
    } else if (action.type === "operation") {
      compilationTargetType =
        dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION;
    } else if (action.type === "assertion") {
      compilationTargetType =
        dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_ASSERTION;
    }

    const jitRequest = dataform.JitCompilationRequest.create({
      target: action.target,
      dependencies: action.dependencyTargets,
      jitCode: action.jitCode,
      compilationTargetType,
      jitData: this.graph.jitData,
    });

    let cancelHandler: ((...args: any[]) => void) | undefined;
    try {
      const jitResponse = await this.executionOptions.jitCompiler({
        request: jitRequest,
        projectDir: this.executionOptions.projectDir,
        dbadapter: this.dbadapter,
        dbclient: client,
        timeoutMillis: this.graph.runConfig?.jitTimeoutMillis || undefined,
        bigqueryOptions: this.getBigQueryExecutionOptions(action),
        onCancel: (handleCancel) => {
          cancelHandler = handleCancel;
          this.eEmitter.on(CANCEL_EVENT, handleCancel);
        },
        expectedCoreVersion: this.executionOptions.expectedCoreVersion,
      });
      action.tasks = this.createTasksFromJitResponse(action, jitResponse);
      const query = actionDryRunValidationQuery(
        jitResponse.table || jitResponse.incrementalTable?.regular || jitResponse.assertion,
      );
      if (query) {
        this.jitDryRunValidationQueries.set(dryRunValidationQueryKey(action.target), query);
      }
    } finally {
      if (cancelHandler) {
        this.eEmitter.removeListener(CANCEL_EVENT, cancelHandler);
      }
    }
  }

  private createTasksFromJitResponse(
    action: dataform.IExecutionAction,
    jitResponse: dataform.IJitCompilationResponse,
  ): dataform.IExecutionTask[] {
    if (jitResponse.table) {
      const table = dataform.Table.create({
        ...action,
        ...jitResponse.table,
        enumType: this.tableTypeEnum(action.tableType),
      });
      return this.executionSql.createTableTasks(
        table,
        this.graph.runConfig,
        this.warehouseStateByTarget.get(targetStringifier.stringify(action.target)),
      );
    } else if (jitResponse.operation) {
      const operation = dataform.Operation.create({
        ...action,
        ...jitResponse.operation,
      });
      return this.executionSql.createOperationTasks(operation);
    } else if (jitResponse.assertion) {
      const assertion = dataform.Assertion.create({
        ...action,
        ...jitResponse.assertion,
      });
      return this.executionSql.createAssertionTasks(assertion);
    } else if (jitResponse.incrementalTable) {
      const table = dataform.Table.create({
        ...action,
        ...jitResponse.incrementalTable.regular,
        incrementalQuery: jitResponse.incrementalTable.incremental?.query,
        incrementalPreOps: jitResponse.incrementalTable.incremental?.preOps,
        incrementalPostOps: jitResponse.incrementalTable.incremental?.postOps,
        enumType: dataform.TableType.INCREMENTAL,
      });
      return this.executionSql.createTableTasks(
        table,
        this.graph.runConfig,
        this.warehouseStateByTarget.get(targetStringifier.stringify(action.target)),
      );
    }
    return [];
  }

  private tableTypeEnum(tableType?: string): dataform.TableType {
    switch (tableType) {
      case "view":
        return dataform.TableType.VIEW;
      case "incremental":
        return dataform.TableType.INCREMENTAL;
      default:
        return dataform.TableType.TABLE;
    }
  }
}

class Timer {
  public static start(existingTiming?: dataform.ITiming) {
    return new Timer(existingTiming?.startTimeMillis.toNumber() || new Date().valueOf());
  }
  private constructor(readonly startTimeMillis: number) {}

  public current(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
    };
  }

  public end(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
      endTimeMillis: Long.fromNumber(new Date().valueOf()),
    };
  }
}
