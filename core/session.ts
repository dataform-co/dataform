import { default as TarjanGraphConstructor, Graph as TarjanGraph } from "tarjan-graph";

import { encode64, verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { Action, ActionProto, ILegacyTableConfig, ITableContext, TableType } from "df/core/actions";
import { AContextable, Assertion, AssertionContext } from "df/core/actions/assertion";
import {
  DataPreparation,
  DataPreparationContext,
  IDataPreparationContext
} from "df/core/actions/data_preparation";
import { Declaration } from "df/core/actions/declaration";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation, OperationContext } from "df/core/actions/operation";
import { Table, TableContext } from "df/core/actions/table";
import { Test } from "df/core/actions/test";
import { View } from "df/core/actions/view";
import { Contextable, ICommonContext, ITarget, Resolvable } from "df/core/common";
import { CompilationSql } from "df/core/compilation_sql";
import { targetAsReadableString, targetStringifier } from "df/core/targets";
import * as utils from "df/core/utils";
import { toResolvable } from "df/core/utils";
import { version as dataformCoreVersion } from "df/core/version";
import { dataform } from "df/protos/ts";

const DEFAULT_CONFIG = {
  defaultSchema: "dataform",
  assertionSchema: "dataform_assertions"
};

/**
 * Contains methods that are published globally, so can be invoked anywhere in the `/definitions`
 * folder of a Dataform project.
 */
export class Session {
  public rootDir: string;

  /**
   * Stores the Dataform project configuration of the current Dataform project. Can be accessed via
   * the `dataform` global variable.
   *
   * Example:
   *
   * ```js
   * dataform.projectConfig.vars.myVariableName === "myVariableValue"
   * ```
   */
  public projectConfig: dataform.ProjectConfig;
  // The canonical project config contains the project config before schema and database overrides.
  public canonicalProjectConfig: dataform.ProjectConfig;

  public actions: Action[];
  public indexedActions: ActionMap;
  public tests: { [name: string]: Test };

  // This map holds information about what assertions are dependent
  // upon a certain action in our actions list. We use this later to resolve dependencies.
  public actionAssertionMap = new ActionMap([]);

  public graphErrors: dataform.IGraphErrors;

  constructor(
    rootDir?: string,
    projectConfig?: dataform.ProjectConfig,
    originalProjectConfig?: dataform.ProjectConfig
  ) {
    this.init(rootDir, projectConfig, originalProjectConfig);
  }

  public init(
    rootDir: string,
    projectConfig?: dataform.ProjectConfig,
    originalProjectConfig?: dataform.ProjectConfig
  ) {
    this.rootDir = rootDir;
    this.projectConfig = dataform.ProjectConfig.create(projectConfig || DEFAULT_CONFIG);
    this.canonicalProjectConfig = getCanonicalProjectConfig(
      dataform.ProjectConfig.create(originalProjectConfig || projectConfig || DEFAULT_CONFIG)
    );
    this.actions = [];
    this.tests = {};
    this.graphErrors = { compilationErrors: [] };
  }

  public compilationSql(): CompilationSql {
    return new CompilationSql(this.projectConfig, dataformCoreVersion);
  }

  public sqlxAction(actionOptions: {
    // sqlxConfig has type any here because any object can be passed in from the compiler - the
    // structure of it is verified at later steps.
    sqlxConfig: any;
    sqlStatementCount: number;
    sqlContextable: (
      ctx:
        | TableContext
        | AssertionContext
        | OperationContext
        | DataPreparationContext
        | IDataPreparationContext
        | ICommonContext
    ) => string[];
    incrementalWhereContextable: (ctx: ITableContext) => string;
    preOperationsContextable: (ctx: ITableContext) => string[];
    postOperationsContextable: (ctx: ITableContext) => string[];
    inputContextables: [
      {
        refName: string[];
        contextable: (ctx: ICommonContext) => string;
      }
    ];
  }) {
    const { sqlxConfig } = actionOptions;
    const actionType = sqlxConfig.hasOwnProperty("type") ? sqlxConfig.type : "operations";
    if (actionOptions.sqlStatementCount > 1 && actionType !== "operations") {
      this.compileError(
        "Actions may only contain more than one SQL statement if they are of type 'operations'."
      );
    }
    if (sqlxConfig.hasOwnProperty("protected") && actionType !== "incremental") {
      this.compileError(
        "Actions may only specify 'protected: true' if they are of type 'incremental'."
      );
    }
    if (actionOptions.incrementalWhereContextable && actionType !== "incremental") {
      this.compileError(
        "Actions may only include incremental_where if they are of type 'incremental'."
      );
    }
    if (!sqlxConfig.hasOwnProperty("schema") && actionType === "declaration") {
      this.compileError("Actions of type 'declaration' must specify a value for 'schema'.");
    }
    if (actionOptions.inputContextables.length > 0 && actionType !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (actionOptions.preOperationsContextable && !definesDataset(actionType)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.postOperationsContextable && !definesDataset(actionType)) {
      this.compileError("Actions may only include post_operations if they create a dataset.");
    }

    switch (actionType) {
      case "view":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        const view = new View(this, sqlxConfig).query(ctx => actionOptions.sqlContextable(ctx)[0]);
        if (actionOptions.incrementalWhereContextable) {
          view.where(actionOptions.incrementalWhereContextable);
        }
        if (actionOptions.preOperationsContextable) {
          view.preOps(actionOptions.preOperationsContextable);
        }
        if (actionOptions.postOperationsContextable) {
          view.postOps(actionOptions.postOperationsContextable);
        }
        this.actions.push(view);
        break;
      case "incremental":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        const incrementalTable = new IncrementalTable(this, sqlxConfig).query(
          ctx => actionOptions.sqlContextable(ctx)[0]
        );
        if (actionOptions.incrementalWhereContextable) {
          incrementalTable.where(actionOptions.incrementalWhereContextable);
        }
        if (actionOptions.preOperationsContextable) {
          incrementalTable.preOps(actionOptions.preOperationsContextable);
        }
        if (actionOptions.postOperationsContextable) {
          incrementalTable.postOps(actionOptions.postOperationsContextable);
        }
        this.actions.push(incrementalTable);
        break;
      case "table":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        const table = new Table(this, sqlxConfig).query(
          ctx => actionOptions.sqlContextable(ctx)[0]
        );
        if (actionOptions.incrementalWhereContextable) {
          table.where(actionOptions.incrementalWhereContextable);
        }
        if (actionOptions.preOperationsContextable) {
          table.preOps(actionOptions.preOperationsContextable);
        }
        if (actionOptions.postOperationsContextable) {
          table.postOps(actionOptions.postOperationsContextable);
        }
        this.actions.push(table);
        break;
      case "assertion":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        this.actions.push(
          new Assertion(this, sqlxConfig).query(ctx => actionOptions.sqlContextable(ctx)[0])
        );
        break;
      case "dataPreparation":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        const dataPreparation = new DataPreparation(this, sqlxConfig).query(
          ctx => actionOptions.sqlContextable(ctx)[0]
        );
        this.actions.push(dataPreparation);
        break;
      case "operations":
        sqlxConfig.filename = utils.getCallerFile(this.rootDir);
        this.actions.push(new Operation(this, sqlxConfig).queries(actionOptions.sqlContextable));
        break;
      case "declaration":
        const declaration = new Declaration(this, sqlxConfig);
        declaration.proto.fileName = utils.getCallerFile(this.rootDir);
        this.actions.push(declaration);
        break;
      case "test":
        const testCase = this.test(sqlxConfig.name)
          .config(sqlxConfig)
          .expect(ctx => actionOptions.sqlContextable(ctx)[0]);
        actionOptions.inputContextables.forEach(({ refName, contextable }) => {
          testCase.input(refName, contextable);
        });
        break;
      default:
        throw new Error(`Unrecognized action type: ${sqlxConfig.type}`);
    }
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]): string {
    ref = toResolvable(ref, rest);
    const allResolved = this.indexedActions.find(utils.resolvableAsTarget(ref));
    if (allResolved.length > 1) {
      this.compileError(new Error(utils.ambiguousActionNameMsg(ref, allResolved)));
      return "";
    }
    const resolved = allResolved.length > 0 ? allResolved[0] : undefined;

    if (resolved && resolved instanceof Operation && !resolved.proto.hasOutput) {
      this.compileError(
        new Error("Actions cannot resolve operations which do not produce output.")
      );
      return "";
    }

    if (resolved) {
      if (resolved instanceof Declaration) {
        return this.compilationSql().resolveTarget(resolved.proto.target);
      }
      return this.compilationSql().resolveTarget({
        ...resolved.proto.target,
        database:
          resolved.proto.target.database && this.finalizeDatabase(resolved.proto.target.database),
        schema: this.finalizeSchema(resolved.proto.target.schema),
        name: this.finalizeName(resolved.proto.target.name)
      });
    }

    this.compileError(new Error(`Could not resolve ${JSON.stringify(ref)}`));
    return "";
  }

  /**
   * Defines a SQL operation.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [operation](Operation) for examples on how to use.
   */
  public operate(
    name: string,
    queryOrConfig?:
      | Contextable<ICommonContext, string | string[]>
      | dataform.ActionConfig.OperationConfig
  ): Operation {
    let operation: Operation;
    if (!!queryOrConfig && typeof queryOrConfig === "object") {
      operation = new Operation(this, { name, ...queryOrConfig });
    } else {
      operation = new Operation(this, { name });
      if (queryOrConfig) {
        operation.queries(queryOrConfig as AContextable<string>);
      }
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(operation);
    return operation;
  }

  /**
   * Creates a table, view, or incremental table.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [Operation](Operation) for examples on how to make tables.
   * @see [View](View) for examples on how to make views.
   * @see [IncrementalTable](IncrementalTable) for examples on how to make incremental tables.
   */
  public publish(
    name: string,
    queryOrConfig?:
      | Contextable<ITableContext, string>
      | dataform.ActionConfig.TableConfig
      | dataform.ActionConfig.ViewConfig
      | dataform.ActionConfig.IncrementalTableConfig
      | ILegacyTableConfig
      // `any` is used here to facilitate the type merging of legacy table configs, which are very
      // different to the new structures.
      | any
  ): Table | IncrementalTable | View {
    // In v4, consider replacing publish with separate methods for each action type.
    let newTable: Table | IncrementalTable | View = new View(this, { type: "view", name });
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        if (queryOrConfig?.type === "view" || queryOrConfig.type === undefined) {
          newTable = new View(this, { type: "view", name, ...queryOrConfig });
        } else if (queryOrConfig?.type === "incremental") {
          newTable = new IncrementalTable(this, { type: "incremental", name, ...queryOrConfig });
        } else if (queryOrConfig?.type === "table") {
          newTable = new Table(this, { type: "table", name, ...queryOrConfig });
        } else {
          throw Error(`Unrecognized table type: ${queryOrConfig.type}`);
        }
      } else {
        // The queryOrConfig is not an object, so it must be a string query.
        newTable.query(queryOrConfig);
      }
    }
    newTable.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(newTable);
    return newTable;
  }

  /**
   * Adds a Dataform assertion the compiled graph.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [assertion](Assertion) for examples on how to use.
   */
  public assert(
    name: string,
    queryOrConfig?: AContextable<string> | dataform.ActionConfig.AssertionConfig
  ): Assertion {
    let assertion: Assertion;
    if (!!queryOrConfig && typeof queryOrConfig === "object") {
      assertion = new Assertion(this, { name, ...queryOrConfig });
    } else {
      assertion = new Assertion(this, { name });
      if (queryOrConfig) {
        assertion.query(queryOrConfig as AContextable<string>);
      }
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(assertion);
    return assertion;
  }

  /**
   * Declares the dataset as a Dataform data source.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [Declaration](Declaration) for examples on how to use.
   */
  public declare(
    config:
      | dataform.ActionConfig.DeclarationConfig
      // `any` is used here to facilitate the type merging of legacy declaration configs options,
      // without breaking typescript consumers of Dataform.
      | any
  ): Declaration {
    const declaration = new Declaration(this, config);
    declaration.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(declaration);
    return declaration;
  }

  /**
   * Creates a Test action.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [Test](Test) for examples on how to use.
   *
   * <!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
   * publish. -->
   * <!-- TODO(ekrekr): add tests for this method -->
   */
  public test(name: string): Test {
    const newTest = new Test();
    newTest.session = this;
    newTest.proto.name = name;
    newTest.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tests[name] = newTest;
    return newTest;
  }

  /**
   * Creates a Notebook action.
   *
   * Available only in the `/definitions` directory.
   *
   * @see [Notebook](Notebook) for examples on how to use.
   *
   * <!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
   * publish. -->
   * <!-- TODO(ekrekr): add tests for this method -->
   */
  public notebook(name: string): Notebook {
    const notebook = new Notebook();
    notebook.session = this;
    utils.setNameAndTarget(this, notebook.proto, name);
    notebook.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(notebook);
    return notebook;
  }

  public compileError(err: Error | string, path?: string, actionTarget?: dataform.ITarget) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;

    const compileError = dataform.CompilationError.create({
      fileName,
      actionName: !!actionTarget ? targetAsReadableString(actionTarget) : undefined,
      actionTarget
    });
    if (typeof err === "string") {
      compileError.message = err;
    } else {
      compileError.message = err.message;
      compileError.stack = err.stack;
    }
    this.graphErrors.compilationErrors.push(compileError);
  }

  public compile(): dataform.CompiledGraph {
    this.indexedActions = new ActionMap(this.actions);

    if (
      (this.projectConfig.warehouse === "bigquery" || this.projectConfig.warehouse === "") &&
      !this.projectConfig.defaultLocation
    ) {
      this.compileError(
        "A defaultLocation is required for BigQuery. This can be configured in workflow_settings.yaml.",
        "workflow_settings.yaml"
      );
    }

    if (
      !!this.projectConfig.vars &&
      !Object.values(this.projectConfig.vars).every(value => typeof value === "string")
    ) {
      throw new Error("Custom variables defined in workflow settings can only be strings.");
    }

    // TODO(ekrekr): replace verify here with something that actually works.
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.projectConfig,
      tables: this.compileGraphChunk(
        this.actions.filter(
          action =>
            action instanceof Table || action instanceof View || action instanceof IncrementalTable
        )
      ),
      operations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Operation)
      ),
      assertions: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Assertion)
      ),
      declarations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Declaration)
      ),
      tests: this.compileGraphChunk(Object.values(this.tests)),
      notebooks: this.compileGraphChunk(this.actions.filter(action => action instanceof Notebook)),
      dataPreparations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof DataPreparation)
      ),
      graphErrors: this.graphErrors,
      dataformCoreVersion,
      targets: this.actions.map(action => action.proto.target)
    });

    this.fullyQualifyDependencies(
      [].concat(
        compiledGraph.tables,
        compiledGraph.assertions,
        compiledGraph.operations,
        compiledGraph.notebooks,
        compiledGraph.dataPreparations
      )
    );

    this.alterActionName(
      [].concat(
        compiledGraph.tables,
        compiledGraph.assertions,
        compiledGraph.operations,
        compiledGraph.notebooks,
        compiledGraph.dataPreparations
      ),
      [].concat(compiledGraph.declarations.map(declaration => declaration.target))
    );

    this.removeNonUniqueActionsFromCompiledGraph(compiledGraph);

    this.checkTestNameUniqueness(compiledGraph.tests);

    this.checkTableConfigValidity(compiledGraph.tables);

    this.checkCircularity(
      [].concat(
        compiledGraph.tables,
        compiledGraph.assertions,
        compiledGraph.operations,
        compiledGraph.notebooks,
        compiledGraph.dataPreparations
      )
    );

    verifyObjectMatchesProto(
      dataform.CompiledGraph,
      compiledGraph,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
    return compiledGraph;
  }

  public compileToBase64() {
    return encode64(dataform.CompiledGraph, this.compile());
  }

  public finalizeDatabase(database: string): string {
    return `${database}${this.getDatabaseSuffixWithUnderscore()}`;
  }

  public finalizeSchema(schema: string): string {
    return `${schema}${this.getSchemaSuffixWithUnderscore()}`;
  }

  public finalizeName(name: string): string {
    return `${this.getTablePrefixWithUnderscore()}${name}`;
  }

  private getDatabaseSuffixWithUnderscore() {
    return !!this.projectConfig.databaseSuffix ? `_${this.projectConfig.databaseSuffix}` : "";
  }

  private getSchemaSuffixWithUnderscore() {
    return !!this.projectConfig.schemaSuffix ? `_${this.projectConfig.schemaSuffix}` : "";
  }

  private getTablePrefixWithUnderscore() {
    return !!this.projectConfig.tablePrefix ? `${this.projectConfig.tablePrefix}_` : "";
  }

  private compileGraphChunk<T>(actions: Array<Action | Test>): T[] {
    const compiledChunks: T[] = [];

    actions.forEach(action => {
      try {
        const compiledChunk = action.compile();
        compiledChunks.push(compiledChunk as any);
      } catch (e) {
        this.compileError(e, action.getFileName(), action.getTarget());
      }
    });

    return compiledChunks;
  }

  private fullyQualifyDependencies(actions: ActionProto[]) {
    actions.forEach(action => {
      const fullyQualifiedDependencies: { [name: string]: dataform.ITarget } = {};
      if (action instanceof dataform.Declaration || !action.dependencyTargets) {
        // Declarations cannot have dependencies.
        return;
      }
      for (const dependency of action.dependencyTargets) {
        const possibleDeps = this.indexedActions.find(dependency);
        if (possibleDeps.length === 0) {
          // We couldn't find a matching target.
          this.compileError(
            new Error(
              `Missing dependency detected: Action "${targetAsReadableString(
                action.target
              )}" depends on "${utils.stringifyResolvable(dependency)}" which does not exist`
            ),
            action.fileName,
            action.target
          );
        } else if (possibleDeps.length === 1) {
          // We found a single matching target, and fully-qualify it if it's a normal dependency.
          const protoDep = possibleDeps[0].proto;
          fullyQualifiedDependencies[targetAsReadableString(protoDep.target)] = protoDep.target;

          if (dependency.includeDependentAssertions) {
            this.actionAssertionMap
              .find(dependency)
              .forEach(
                assertion =>
                  (fullyQualifiedDependencies[targetAsReadableString(assertion.proto.target)] =
                    assertion.proto.target)
              );
          }
        } else {
          // Too many targets matched the dependency.
          this.compileError(
            new Error(utils.ambiguousActionNameMsg(dependency, possibleDeps)),
            action.fileName,
            action.target
          );
        }
      }
      action.dependencyTargets = Object.values(fullyQualifiedDependencies);
    });
  }

  private alterActionName(actions: ActionProto[], declarationTargets: dataform.ITarget[]) {
    const { tablePrefix, schemaSuffix, databaseSuffix } = this.projectConfig;

    if (!tablePrefix && !schemaSuffix && !databaseSuffix) {
      return;
    }

    const newTargetByOriginalTarget = new Map<string, dataform.ITarget>();
    declarationTargets.forEach(declarationTarget =>
      newTargetByOriginalTarget.set(
        targetStringifier.stringify(declarationTarget),
        declarationTarget
      )
    );

    actions.forEach(action => {
      newTargetByOriginalTarget.set(targetStringifier.stringify(action.target), {
        ...action.target,
        database:
          action.target.database &&
          `${action.target.database}${this.getDatabaseSuffixWithUnderscore()}`,
        schema: `${action.target.schema}${this.getSchemaSuffixWithUnderscore()}`,
        name: `${this.getTablePrefixWithUnderscore()}${action.target.name}`
      });
      action.target = newTargetByOriginalTarget.get(targetStringifier.stringify(action.target));
    });

    // Fix up dependencies in case those dependencies' names have changed.
    const getUpdatedTarget = (originalTarget: dataform.ITarget) => {
      // It's possible that we don't have a new Target for a dependency that failed to compile,
      // so fall back to the original Target.
      if (!newTargetByOriginalTarget.has(targetStringifier.stringify(originalTarget))) {
        return originalTarget;
      }
      return newTargetByOriginalTarget.get(targetStringifier.stringify(originalTarget));
    };
    actions.forEach(action => {
      if (!(action instanceof dataform.Declaration)) {
        // Declarations cannot have dependencies.
        action.dependencyTargets = (action.dependencyTargets || []).map(getUpdatedTarget);
      }

      if (action instanceof dataform.Assertion && !!action.parentAction) {
        action.parentAction = getUpdatedTarget(action.parentAction);
      }
    });
  }

  // TODO(ekrekr): finish pushing config validation down to the classes.
  private checkTableConfigValidity(tables: dataform.ITable[]) {
    tables.forEach(table => {
      // type
      if (table.enumType === dataform.TableType.UNKNOWN_TYPE) {
        this.compileError(
          `Wrong type of table detected. Should only use predefined types: ${joinQuoted(
            TableType
          )}`,
          table.fileName,
          table.target
        );
      }

      // materialized
      if (!!table.materialized) {
        if (table.enumType !== dataform.TableType.VIEW) {
          this.compileError(
            new Error(`The 'materialized' option is only valid for BigQuery views`),
            table.fileName,
            table.target
          );
        }
      }

      // BigQuery config
      if (!!table.bigquery) {
        if (
          (table.bigquery.partitionBy ||
            table.bigquery.clusterBy?.length ||
            table.bigquery.partitionExpirationDays ||
            table.bigquery.requirePartitionFilter) &&
          table.enumType === dataform.TableType.VIEW &&
          !table.materialized
        ) {
          this.compileError(
            `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`,
            table.fileName,
            table.target
          );
        } else if (
          (table.bigquery.partitionExpirationDays || table.bigquery.requirePartitionFilter) &&
          table.enumType === dataform.TableType.VIEW &&
          table.materialized
        ) {
          this.compileError(
            `requirePartitionFilter/partitionExpirationDays are not valid for BigQuery materialized views`,
            table.fileName,
            table.target
          );
        } else if (
          !table.bigquery.partitionBy &&
          (table.bigquery.partitionExpirationDays || table.bigquery.requirePartitionFilter) &&
          table.enumType === dataform.TableType.TABLE
        ) {
          this.compileError(
            `requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables`,
            table.fileName,
            table.target
          );
        } else if (table.bigquery.additionalOptions) {
          if (
            table.bigquery.partitionExpirationDays &&
            table.bigquery.additionalOptions.partition_expiration_days
          ) {
            this.compileError(
              `partitionExpirationDays has been declared twice`,
              table.fileName,
              table.target
            );
          }
          if (
            table.bigquery.requirePartitionFilter &&
            table.bigquery.additionalOptions.require_partition_filter
          ) {
            this.compileError(
              `requirePartitionFilter has been declared twice`,
              table.fileName,
              table.target
            );
          }
        }
      }
    });
  }

  private checkTestNameUniqueness(tests: dataform.ITest[]) {
    const allNames: string[] = [];
    tests.forEach(testProto => {
      if (allNames.includes(testProto.name)) {
        this.compileError(
          new Error(`Duplicate test name detected: "${testProto.name}"`),
          testProto.fileName
        );
      }
      allNames.push(testProto.name);
    });
  }

  private checkCircularity(actions: ActionProto[]) {
    const allActionsByStringifiedTarget = new Map<string, ActionProto>(
      actions.map(action => [targetStringifier.stringify(action.target), action])
    );

    // Type exports for tarjan-graph are unfortunately wrong, so we have to do this minor hack.
    const tarjanGraph: TarjanGraph = new (TarjanGraphConstructor as any)();
    actions.forEach(action => {
      // Declarations cannot have dependencies.
      const cleanedDependencies = (action instanceof dataform.Declaration ||
      !action.dependencyTargets
        ? []
        : action.dependencyTargets
      ).filter(
        dependency => !!allActionsByStringifiedTarget.get(targetStringifier.stringify(dependency))
      );
      tarjanGraph.add(
        targetStringifier.stringify(action.target),
        cleanedDependencies.map(target => targetStringifier.stringify(target))
      );
    });
    const cycles = tarjanGraph.getCycles();
    cycles.forEach(cycle => {
      const firstActionInCycle = allActionsByStringifiedTarget.get(cycle[0].name);
      const message = `Circular dependency detected in chain: [${cycle
        .map(vertex => vertex.name)
        .join(" > ")} > ${targetAsReadableString(firstActionInCycle.target)}]`;
      this.compileError(new Error(message), firstActionInCycle.fileName, firstActionInCycle.target);
    });
  }

  private removeNonUniqueActionsFromCompiledGraph(compiledGraph: dataform.CompiledGraph) {
    function getNonUniqueTargets(targets: dataform.ITarget[]): Set<string> {
      const allTargets = new Set<string>();
      const nonUniqueTargets = new Set<string>();

      targets.forEach(target => {
        if (allTargets.has(targetStringifier.stringify(target))) {
          nonUniqueTargets.add(targetStringifier.stringify(target));
        }
        allTargets.add(targetStringifier.stringify(target));
      });

      return nonUniqueTargets;
    }

    const actions = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations,
      compiledGraph.declarations,
      compiledGraph.notebooks,
      compiledGraph.dataPreparations
    );

    const nonUniqueActionsTargets = getNonUniqueTargets(actions.map(action => action.target));
    const nonUniqueActionsCanonicalTargets = getNonUniqueTargets(
      actions.map(action => action.canonicalTarget)
    );

    const isUniqueAction = (action: ActionProto) => {
      const isNonUniqueTarget = nonUniqueActionsTargets.has(
        targetStringifier.stringify(action.target)
      );
      const isNonUniqueCanonicalTarget = nonUniqueActionsCanonicalTargets.has(
        targetStringifier.stringify(action.canonicalTarget)
      );

      if (isNonUniqueTarget) {
        this.compileError(
          new Error(
            `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n"${JSON.stringify(
              action.target
            )}"`
          ),
          action.fileName,
          action.target
        );
      }
      if (isNonUniqueCanonicalTarget) {
        this.compileError(
          new Error(
            `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n"${JSON.stringify(
              action.canonicalTarget
            )}"`
          ),
          action.fileName,
          action.target
        );
      }

      return !isNonUniqueTarget && !isNonUniqueCanonicalTarget;
    };

    compiledGraph.tables = compiledGraph.tables.filter(isUniqueAction);
    compiledGraph.operations = compiledGraph.operations.filter(isUniqueAction);
    compiledGraph.declarations = compiledGraph.declarations.filter(isUniqueAction);
    compiledGraph.assertions = compiledGraph.assertions.filter(isUniqueAction);
    compiledGraph.notebooks = compiledGraph.notebooks.filter(isUniqueAction);
    compiledGraph.dataPreparations = compiledGraph.dataPreparations.filter(isUniqueAction);
  }
}

function definesDataset(type: string) {
  return type === "view" || type === "table" || type === "incremental";
}

function getCanonicalProjectConfig(originalProjectConfig: dataform.ProjectConfig) {
  return dataform.ProjectConfig.create({
    warehouse: originalProjectConfig.warehouse,
    defaultSchema: originalProjectConfig.defaultSchema,
    defaultDatabase: originalProjectConfig.defaultDatabase,
    assertionSchema: originalProjectConfig.assertionSchema
  });
}

function joinQuoted(values: readonly string[]) {
  return values.map((value: string) => `"${value}"`).join(" | ");
}

class ActionMap {
  private byName: Map<string, Action[]> = new Map();
  private bySchemaAndName: Map<string, Map<string, Action[]>> = new Map();
  private byDatabaseAndName: Map<string, Map<string, Action[]>> = new Map();
  private byDatabaseSchemaAndName: Map<string, Map<string, Map<string, Action[]>>> = new Map();

  public constructor(actions: Action[]) {
    for (const action of actions) {
      this.set(action.proto.target, action);
    }
  }

  public set(actionTarget: ITarget, assertionTarget: Action) {
    this.setByNameLevel(this.byName, actionTarget.name, assertionTarget);

    if (!!actionTarget.schema) {
      this.setBySchemaLevel(this.bySchemaAndName, actionTarget, assertionTarget);
    }

    if (!!actionTarget.database) {
      if (!this.byDatabaseAndName.has(actionTarget.database)) {
        this.byDatabaseAndName.set(actionTarget.database, new Map());
      }
      const forDatabaseNoSchema = this.byDatabaseAndName.get(actionTarget.database);
      this.setByNameLevel(forDatabaseNoSchema, actionTarget.name, assertionTarget);

      if (!!actionTarget.schema) {
        if (!this.byDatabaseSchemaAndName.has(actionTarget.database)) {
          this.byDatabaseSchemaAndName.set(actionTarget.database, new Map());
        }
        const forDatabase = this.byDatabaseSchemaAndName.get(actionTarget.database);
        this.setBySchemaLevel(forDatabase, actionTarget, assertionTarget);
      }
    }
  }

  public find(target: dataform.ITarget) {
    if (!!target.database) {
      if (!!target.schema) {
        return (
          this.byDatabaseSchemaAndName
            .get(target.database)
            ?.get(target.schema)
            ?.get(target.name) || []
        );
      }
      return this.byDatabaseAndName.get(target.database)?.get(target.name) || [];
    }
    if (!!target.schema) {
      return this.bySchemaAndName.get(target.schema)?.get(target.name) || [];
    }
    return this.byName.get(target.name) || [];
  }

  private setByNameLevel(targetMap: Map<string, Action[]>, name: string, assertionTarget: Action) {
    if (!targetMap.has(name)) {
      targetMap.set(name, []);
    }
    targetMap.get(name).push(assertionTarget);
  }

  private setBySchemaLevel(
    targetMap: Map<string, Map<string, Action[]>>,
    actionTarget: ITarget,
    assertionTarget: Action
  ) {
    if (!targetMap.has(actionTarget.schema)) {
      targetMap.set(actionTarget.schema, new Map());
    }
    const forSchema = targetMap.get(actionTarget.schema);
    this.setByNameLevel(forSchema, actionTarget.name, assertionTarget);
  }
}
