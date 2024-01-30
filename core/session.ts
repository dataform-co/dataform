import { default as TarjanGraphConstructor, Graph as TarjanGraph } from "tarjan-graph";

import { encode64 } from "df/common/protos";
import { StringifiedMap, StringifiedSet } from "df/common/strings/stringifier";
import * as adapters from "df/core/adapters";
import { AContextable, Assertion, AssertionContext, IAssertionConfig } from "df/core/assertion";
import { Contextable, ICommonContext, Resolvable } from "df/core/common";
import { Declaration, IDeclarationConfig } from "df/core/declaration";
import { IOperationConfig, Operation, OperationContext } from "df/core/operation";
import {
  DistStyleType,
  ITableConfig,
  ITableContext,
  SortStyleType,
  Table,
  TableContext,
  TableType
} from "df/core/table";
import { targetAsReadableString, targetStringifier } from "df/core/targets";
import * as test from "df/core/test";
import * as utils from "df/core/utils";
import { setOrValidateTableEnumType, toResolvable } from "df/core/utils";
import { version as dataformCoreVersion } from "df/core/version";
import { dataform } from "df/protos/ts";

const SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP = new RegExp("HASH\\s*\\(\\s*\\w*\\s*\\)\\s*");

const DEFAULT_CONFIG = {
  defaultSchema: "dataform",
  assertionSchema: "dataform_assertions"
};

/**
 * @hidden
 */
export interface IActionProto {
  fileName?: string;
  dependencyTargets?: dataform.ITarget[];
  hermeticity?: dataform.ActionHermeticity;
  target?: dataform.ITarget;
  canonicalTarget?: dataform.ITarget;
  parentAction?: dataform.ITarget;
}

type SqlxConfig = (
  | (ITableConfig & { type: TableType })
  | (IAssertionConfig & { type: "assertion" })
  | (IOperationConfig & { type: "operations" })
  | (IDeclarationConfig & { type: "declaration" })
  | (test.ITestConfig & { type: "test" })
) & { name: string };

type Action = Table | Operation | Assertion | Declaration;

/**
 * @hidden
 */
export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;
  public canonicalConfig: dataform.IProjectConfig;
  public supportSqlFileCompilation: boolean;

  public actions: Action[];
  public indexedActions: ActionIndex;
  public tests: { [name: string]: test.Test };

  public graphErrors: dataform.IGraphErrors;

  constructor(
    rootDir?: string,
    projectConfig?: dataform.IProjectConfig,
    originalProjectConfig?: dataform.IProjectConfig,
    supportSqlFileCompilation?: boolean
  ) {
    this.init(rootDir, projectConfig, originalProjectConfig, supportSqlFileCompilation);
  }

  public init(
    rootDir: string,
    projectConfig?: dataform.IProjectConfig,
    originalProjectConfig?: dataform.IProjectConfig,
    supportSqlFileCompilation: boolean = true
  ) {
    this.rootDir = rootDir;
    this.config = projectConfig || DEFAULT_CONFIG;
    this.canonicalConfig = getCanonicalProjectConfig(
      originalProjectConfig || projectConfig || DEFAULT_CONFIG
    );
    this.supportSqlFileCompilation = supportSqlFileCompilation;
    this.actions = [];
    this.tests = {};
    this.graphErrors = { compilationErrors: [] };
  }

  public get projectConfig(): Pick<
    dataform.IProjectConfig,
    | "warehouse"
    | "defaultDatabase"
    | "defaultSchema"
    | "assertionSchema"
    | "databaseSuffix"
    | "schemaSuffix"
    | "tablePrefix"
    | "vars"
  > {
    return Object.freeze({
      warehouse: this.config.warehouse,
      defaultDatabase: this.config.defaultDatabase,
      defaultSchema: this.config.defaultSchema,
      assertionSchema: this.config.assertionSchema,
      databaseSuffix: this.config.databaseSuffix,
      schemaSuffix: this.config.schemaSuffix,
      tablePrefix: this.config.tablePrefix,
      vars: Object.freeze({ ...this.config.vars })
    });
  }

  public adapter(): adapters.IAdapter {
    return adapters.create(this.config, dataformCoreVersion);
  }

  public sqlxAction(actionOptions: {
    sqlxConfig: SqlxConfig;
    sqlStatementCount: number;
    sqlContextable: (
      ctx: TableContext | AssertionContext | OperationContext | ICommonContext
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
    if (actionOptions.sqlStatementCount > 1 && sqlxConfig.type !== "operations") {
      this.compileError(
        "Actions may only contain more than one SQL statement if they are of type 'operations'."
      );
    }
    if (sqlxConfig.hasOwnProperty("protected") && sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only specify 'protected: true' if they are of type 'incremental'."
      );
    }
    if (actionOptions.incrementalWhereContextable && sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only include incremental_where if they are of type 'incremental'."
      );
    }
    if (!sqlxConfig.hasOwnProperty("schema") && sqlxConfig.type === "declaration") {
      this.compileError("Actions of type 'declaration' must specify a value for 'schema'.");
    }
    if (actionOptions.inputContextables.length > 0 && sqlxConfig.type !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (actionOptions.preOperationsContextable && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.postOperationsContextable && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only include post_operations if they create a dataset.");
    }
    if (
      !!sqlxConfig.hasOwnProperty("sqldatawarehouse") &&
      !["bigquery", "snowflake"].includes(this.config.warehouse)
    ) {
      this.compileError(
        "Actions may only specify 'database' in projects whose warehouse is 'BigQuery' or 'Snowflake'."
      );
    }

    switch (sqlxConfig.type) {
      case "view":
      case "table":
      case "inline":
      case "incremental":
        const table = this.publish(sqlxConfig.name)
          .config(sqlxConfig)
          .query(ctx => actionOptions.sqlContextable(ctx)[0]);
        if (actionOptions.incrementalWhereContextable) {
          table.where(actionOptions.incrementalWhereContextable);
        }
        if (actionOptions.preOperationsContextable) {
          table.preOps(actionOptions.preOperationsContextable);
        }
        if (actionOptions.postOperationsContextable) {
          table.postOps(actionOptions.postOperationsContextable);
        }
        break;
      case "assertion":
        this.assert(sqlxConfig.name)
          .config(sqlxConfig)
          .query(ctx => actionOptions.sqlContextable(ctx)[0]);
        break;
      case "operations":
        this.operate(sqlxConfig.name)
          .config(sqlxConfig)
          .queries(actionOptions.sqlContextable);
        break;
      case "declaration":
        this.declare({
          database: sqlxConfig.database,
          schema: sqlxConfig.schema,
          name: sqlxConfig.name
        }).config(sqlxConfig);
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
        throw new Error(`Unrecognized action type: ${(sqlxConfig as SqlxConfig).type}`);
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

    if (
      resolved &&
      resolved instanceof Table &&
      resolved.proto.enumType === dataform.TableType.INLINE
    ) {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${resolved.proto.query})`;
    }
    if (resolved && resolved instanceof Operation && !resolved.proto.hasOutput) {
      this.compileError(
        new Error("Actions cannot resolve operations which do not produce output.")
      );
      return "";
    }

    if (resolved) {
      if (resolved instanceof Declaration) {
        return this.adapter().resolveTarget(resolved.proto.target);
      }
      return this.adapter().resolveTarget({
        ...resolved.proto.target,
        database:
          resolved.proto.target.database && this.finalizeDatabase(resolved.proto.target.database),
        schema: this.finalizeSchema(resolved.proto.target.schema),
        name: this.finalizeName(resolved.proto.target.name)
      });
    }

    if (!this.supportSqlFileCompilation) {
      this.compileError(new Error(`Could not resolve ${JSON.stringify(ref)}`));
      return "";
    }

    // TODO: Here we allow 'ref' to go unresolved. This is for backwards compatibility with projects
    // that use .sql files. In these projects, this session may not know about all actions (yet), and
    // thus we need to fall back to assuming that the target *will* exist in the future. Once we break
    // backwards compatibility with .sql files, we should remove the below code, and append a compile
    // error instead.
    if (typeof ref === "string") {
      return this.adapter().resolveTarget(
        utils.target(
          this.adapter(),
          this.config,
          this.finalizeName(ref),
          this.finalizeSchema(this.config.defaultSchema),
          this.config.defaultDatabase && this.finalizeDatabase(this.config.defaultDatabase)
        )
      );
    }
    return this.adapter().resolveTarget(
      utils.target(
        this.adapter(),
        this.config,
        this.finalizeName(ref.name),
        this.finalizeSchema(ref.schema),
        ref.database && this.finalizeName(ref.database)
      )
    );
  }

  public operate(
    name: string,
    queries?: Contextable<ICommonContext, string | string[]>
  ): Operation {
    const operation = new Operation();
    operation.session = this;
    utils.setNameAndTarget(this, operation.proto, name);
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(operation);
    return operation;
  }

  public publish(
    name: string,
    queryOrConfig?: Contextable<ITableContext, string> | ITableConfig
  ): Table {
    const newTable = new Table();
    newTable.session = this;
    utils.setNameAndTarget(this, newTable.proto, name);
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        newTable.config(queryOrConfig);
      } else {
        newTable.query(queryOrConfig);
      }
    }
    newTable.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(newTable);
    return newTable;
  }

  public assert(name: string, query?: AContextable<string>): Assertion {
    const assertion = new Assertion();
    assertion.session = this;
    utils.setNameAndTarget(this, assertion.proto, name, this.config.assertionSchema);
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(assertion);
    return assertion;
  }

  public declare(dataset: dataform.ITarget): Declaration {
    const declaration = new Declaration();
    declaration.session = this;
    utils.setNameAndTarget(this, declaration.proto, dataset.name, dataset.schema, dataset.database);
    declaration.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(declaration);
    return declaration;
  }

  public test(name: string): test.Test {
    const newTest = new test.Test();
    newTest.session = this;
    newTest.proto.name = name;
    newTest.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tests[name] = newTest;
    return newTest;
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
    this.indexedActions = new ActionIndex(this.adapter(), this.actions);

    if (this.config.warehouse === "bigquery" && !this.config.defaultLocation) {
      this.compileError(
        "A defaultLocation is required for BigQuery. This can be configured in dataform.json.",
        "dataform.json"
      );
    }
    if (
      !!this.config.vars &&
      !Object.values(this.config.vars).every(value => typeof value === "string")
    ) {
      throw new Error("Custom variables defined in dataform.json can only be strings.");
    }

    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Table),
        dataform.Table.verify
      ),
      operations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Operation),
        dataform.Operation.verify
      ),
      assertions: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Assertion),
        dataform.Assertion.verify
      ),
      declarations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Declaration),
        dataform.Declaration.verify
      ),
      tests: this.compileGraphChunk(Object.values(this.tests), dataform.Test.verify),
      graphErrors: this.graphErrors,
      dataformCoreVersion,
      targets: this.actions.map(action => action.proto.target)
    });

    this.fullyQualifyDependencies(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
    );

    this.alterActionName(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations),
      [].concat(compiledGraph.declarations.map(declaration => declaration.target))
    );

    this.removeNonUniqueActionsFromCompiledGraph(compiledGraph);

    this.checkTestNameUniqueness(compiledGraph.tests);

    this.checkTableConfigValidity(compiledGraph.tables);

    this.checkCircularity(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
    );

    if (this.config.useRunCache) {
      this.checkRunCachingCorrectness(
        [].concat(
          compiledGraph.tables,
          compiledGraph.assertions,
          compiledGraph.operations.filter(operation => operation.hasOutput)
        )
      );
    }

    utils.throwIfInvalid(compiledGraph, dataform.CompiledGraph.verify);
    return compiledGraph;
  }

  public compileToBase64() {
    return encode64(dataform.CompiledGraph, this.compile());
  }

  public finalizeDatabase(database: string): string {
    return this.adapter().normalizeIdentifier(
      `${database}${this.getDatabaseSuffixWithUnderscore()}`
    );
  }

  public finalizeSchema(schema: string): string {
    return this.adapter().normalizeIdentifier(`${schema}${this.getSchemaSuffixWithUnderscore()}`);
  }

  public finalizeName(name: string): string {
    return this.adapter().normalizeIdentifier(`${this.getTablePrefixWithUnderscore()}${name}`);
  }

  private getDatabaseSuffixWithUnderscore() {
    return !!this.config.databaseSuffix ? `_${this.config.databaseSuffix}` : "";
  }

  private getSchemaSuffixWithUnderscore() {
    return !!this.config.schemaSuffix ? `_${this.config.schemaSuffix}` : "";
  }

  private getTablePrefixWithUnderscore() {
    return !!this.config.tablePrefix ? `${this.config.tablePrefix}_` : "";
  }

  private compileGraphChunk<T>(
    actions: Array<{ proto: IActionProto; compile(): T }>,
    verify: (proto: T) => string
  ): T[] {
    const compiledChunks: T[] = [];

    actions.forEach(action => {
      try {
        const compiledChunk = action.compile();
        utils.throwIfInvalid(compiledChunk, verify);
        compiledChunks.push(compiledChunk);
      } catch (e) {
        this.compileError(e, action.proto.fileName, action.proto.target);
      }
    });

    return compiledChunks;
  }

  private fullyQualifyDependencies(actions: IActionProto[]) {
    actions.forEach(action => {
      const fullyQualifiedDependencies: { [name: string]: dataform.ITarget } = {};
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
          // We found a single matching target, and fully-qualify it if it's a normal dependency,
          // or add all of its dependencies to ours if it's an 'inline' table.
          const protoDep = possibleDeps[0].proto;
          if (
            protoDep instanceof dataform.Table &&
            protoDep.enumType === dataform.TableType.INLINE
          ) {
            protoDep.dependencyTargets.forEach(inlineDep =>
              action.dependencyTargets.push(inlineDep)
            );
          } else {
            fullyQualifiedDependencies[targetAsReadableString(protoDep.target)] = protoDep.target;
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

  private alterActionName(actions: IActionProto[], declarationTargets: dataform.ITarget[]) {
    const { tablePrefix, schemaSuffix, databaseSuffix } = this.config;

    if (!tablePrefix && !schemaSuffix && !databaseSuffix) {
      return;
    }

    const newTargetByOriginalTarget = new StringifiedMap<dataform.ITarget, dataform.ITarget>(
      targetStringifier
    );
    declarationTargets.forEach(declarationTarget =>
      newTargetByOriginalTarget.set(declarationTarget, declarationTarget)
    );

    actions.forEach(action => {
      newTargetByOriginalTarget.set(action.target, {
        ...action.target,
        database:
          action.target.database &&
          this.adapter().normalizeIdentifier(
            `${action.target.database}${this.getDatabaseSuffixWithUnderscore()}`
          ),
        schema: this.adapter().normalizeIdentifier(
          `${action.target.schema}${this.getSchemaSuffixWithUnderscore()}`
        ),
        name: this.adapter().normalizeIdentifier(
          `${this.getTablePrefixWithUnderscore()}${action.target.name}`
        )
      });
      action.target = newTargetByOriginalTarget.get(action.target);
    });

    // Fix up dependencies in case those dependencies' names have changed.
    const getUpdatedTarget = (originalTarget: dataform.ITarget) => {
      // It's possible that we don't have a new Target for a dependency that failed to compile,
      // so fall back to the original Target.
      if (!newTargetByOriginalTarget.has(originalTarget)) {
        return originalTarget;
      }
      return newTargetByOriginalTarget.get(originalTarget);
    };
    actions.forEach(action => {
      action.dependencyTargets = (action.dependencyTargets || []).map(getUpdatedTarget);

      if (!!action.parentAction) {
        action.parentAction = getUpdatedTarget(action.parentAction);
      }
    });
  }

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
        if (
          table.enumType !== dataform.TableType.VIEW ||
          (this.config.warehouse !== "snowflake" && this.config.warehouse !== "bigquery")
        ) {
          this.compileError(
            new Error(`The 'materialized' option is only valid for Snowflake and BigQuery views`),
            table.fileName,
            table.target
          );
        }
      }

      // snowflake config
      if (!!table.snowflake) {
        if (table.snowflake.secure && table.enumType !== dataform.TableType.VIEW) {
          this.compileError(
            new Error(`The 'secure' option is only valid for Snowflake views`),
            table.fileName,
            table.target
          );
        }

        if (table.snowflake.transient && table.enumType !== dataform.TableType.TABLE) {
          this.compileError(
            new Error(`The 'transient' option is only valid for Snowflake tables`),
            table.fileName,
            table.target
          );
        }

        if (
          table.snowflake.clusterBy?.length > 0 &&
          table.enumType !== dataform.TableType.TABLE &&
          table.enumType !== dataform.TableType.INCREMENTAL
        ) {
          this.compileError(
            new Error(`The 'clusterBy' option is only valid for Snowflake tables`),
            table.fileName,
            table.target
          );
        }
      }

      // sqldatawarehouse config
      if (!!table.sqlDataWarehouse) {
        if (!!table.uniqueKey && table.uniqueKey.length > 0) {
          this.compileError(
            new Error(
              `Merging using unique keys for SQLDataWarehouse has not yet been implemented`
            ),
            table.fileName,
            table.target
          );
        }

        if (table.sqlDataWarehouse.distribution) {
          const distribution = table.sqlDataWarehouse.distribution.toUpperCase();
          if (
            distribution !== "REPLICATE" &&
            distribution !== "ROUND_ROBIN" &&
            !SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP.test(distribution)
          ) {
            this.compileError(
              new Error(`Invalid value for sqldatawarehouse distribution: ${distribution}`),
              table.fileName,
              table.target
            );
          }
        }
      }

      // Redshift config
      if (!!table.redshift) {
        const validatePropertyDefined = (
          opts: dataform.IRedshiftOptions,
          prop: keyof dataform.IRedshiftOptions
        ) => {
          const value = opts[prop];
          if (!opts.hasOwnProperty(prop)) {
            this.compileError(`Property "${prop}" is not defined`, table.fileName, table.target);
          } else if (value instanceof Array) {
            if (value.length === 0) {
              this.compileError(`Property "${prop}" is not defined`, table.fileName, table.target);
            }
          }
        };
        const validatePropertiesDefined = (
          opts: dataform.IRedshiftOptions,
          props: Array<keyof dataform.IRedshiftOptions>
        ) => props.forEach(prop => validatePropertyDefined(opts, prop));
        const validatePropertyValueInValues = (
          opts: dataform.IRedshiftOptions,
          prop: keyof dataform.IRedshiftOptions & ("distStyle" | "sortStyle"),
          values: readonly string[]
        ) => {
          if (!!opts[prop] && !values.includes(opts[prop])) {
            this.compileError(
              `Wrong value of "${prop}" property. Should only use predefined values: ${joinQuoted(
                values
              )}`,
              table.fileName,
              table.target
            );
          }
        };

        if (table.redshift.distStyle || table.redshift.distKey) {
          validatePropertiesDefined(table.redshift, ["distStyle", "distKey"]);
          validatePropertyValueInValues(table.redshift, "distStyle", DistStyleType);
        }
        if (
          table.redshift.sortStyle ||
          (table.redshift.sortKeys && table.redshift.sortKeys.length)
        ) {
          validatePropertiesDefined(table.redshift, ["sortStyle", "sortKeys"]);
          validatePropertyValueInValues(table.redshift, "sortStyle", SortStyleType);
        }
      }

      // BigQuery config
      if (!!table.bigquery) {
        if (
          (table.bigquery.partitionBy ||
            table.bigquery.clusterBy?.length ||
            table.bigquery.partitionExpirationDays ||
            table.bigquery.requirePartitionFilter) &&
          table.enumType === dataform.TableType.VIEW
        ) {
          this.compileError(
            `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views; they are only valid for tables`,
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

      // Ignored properties
      if (table.enumType === dataform.TableType.INLINE) {
        Table.INLINE_IGNORED_PROPS.forEach(ignoredProp => {
          if (objectExistsOrIsNonEmpty(table[ignoredProp])) {
            this.compileError(
              `Unused property was detected: "${ignoredProp}". This property is not used for tables with type "inline" and will be ignored`,
              table.fileName,
              table.target
            );
          }
        });
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

  private checkCircularity(actions: IActionProto[]) {
    const allActionsByStringifiedTarget = new Map<string, IActionProto>(
      actions.map(action => [targetStringifier.stringify(action.target), action])
    );

    // Type exports for tarjan-graph are unfortunately wrong, so we have to do this minor hack.
    const tarjanGraph: TarjanGraph = new (TarjanGraphConstructor as any)();
    actions.forEach(action => {
      const cleanedDependencies = (action.dependencyTargets || []).filter(
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

  private checkRunCachingCorrectness(actionsWithOutput: IActionProto[]) {
    actionsWithOutput.forEach(action => {
      if (action.dependencyTargets?.length > 0) {
        return;
      }
      if (
        [dataform.ActionHermeticity.HERMETIC, dataform.ActionHermeticity.NON_HERMETIC].includes(
          action.hermeticity
        )
      ) {
        return;
      }
      this.compileError(
        new Error(
          "Zero-dependency actions which create datasets are required to explicitly declare 'hermetic: (true|false)' when run caching is turned on."
        ),
        action.fileName,
        action.target
      );
    });
  }

  private removeNonUniqueActionsFromCompiledGraph(compiledGraph: dataform.CompiledGraph) {
    function getNonUniqueTargets(targets: dataform.ITarget[]): StringifiedSet<dataform.ITarget> {
      const allTargets = new StringifiedSet<dataform.ITarget>(targetStringifier);
      const nonUniqueTargets = new StringifiedSet<dataform.ITarget>(targetStringifier);

      targets.forEach(target => {
        if (allTargets.has(target)) {
          nonUniqueTargets.add(target);
        }
        allTargets.add(target);
      });

      return nonUniqueTargets;
    }

    const actions = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations,
      compiledGraph.declarations
    );

    const nonUniqueActionsTargets = getNonUniqueTargets(actions.map(action => action.target));
    const nonUniqueActionsCanonicalTargets = getNonUniqueTargets(
      actions.map(action => action.canonicalTarget)
    );

    const isUniqueAction = (action: IActionProto) => {
      const isNonUniqueTarget = nonUniqueActionsTargets.has(action.target);
      const isNonUniqueCanonicalTarget = nonUniqueActionsCanonicalTargets.has(
        action.canonicalTarget
      );

      if (isNonUniqueTarget) {
        this.compileError(
          new Error(
            `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations`
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
  }
}

function declaresDataset(type: string, hasOutput?: boolean) {
  return definesDataset(type) || type === "declaration" || hasOutput;
}

function definesDataset(type: string) {
  return type === "view" || type === "table" || type === "inline" || type === "incremental";
}

function getCanonicalProjectConfig(originalProjectConfig: dataform.IProjectConfig) {
  return {
    warehouse: originalProjectConfig.warehouse,
    defaultSchema: originalProjectConfig.defaultSchema,
    defaultDatabase: originalProjectConfig.defaultDatabase,
    assertionSchema: originalProjectConfig.assertionSchema
  };
}

function joinQuoted(values: readonly string[]) {
  return values.map((value: string) => `"${value}"`).join(" | ");
}

function objectExistsOrIsNonEmpty(prop: any): boolean {
  if (!prop) {
    return false;
  }

  return (
    (Array.isArray(prop) && !!prop.length) ||
    (!Array.isArray(prop) && typeof prop === "object" && !!Object.keys(prop).length) ||
    typeof prop !== "object"
  );
}

class ActionIndex {
  private readonly byName: Map<string, Action[]> = new Map();
  private readonly bySchemaAndName: Map<string, Map<string, Action[]>> = new Map();
  private readonly byDatabaseAndName: Map<string, Map<string, Action[]>> = new Map();
  private readonly byDatabaseSchemaAndName: Map<
    string,
    Map<string, Map<string, Action[]>>
  > = new Map();

  public constructor(private readonly adapter: adapters.IAdapter, actions: Action[]) {
    for (const action of actions) {
      if (!this.byName.has(action.proto.target.name)) {
        this.byName.set(action.proto.target.name, []);
      }
      this.byName.get(action.proto.target.name).push(action);

      if (!this.bySchemaAndName.has(action.proto.target.schema)) {
        this.bySchemaAndName.set(action.proto.target.schema, new Map());
      }
      const forSchema = this.bySchemaAndName.get(action.proto.target.schema);
      if (!forSchema.has(action.proto.target.name)) {
        forSchema.set(action.proto.target.name, []);
      }
      forSchema.get(action.proto.target.name).push(action);

      if (!!action.proto.target.database) {
        if (!this.byDatabaseAndName.has(action.proto.target.database)) {
          this.byDatabaseAndName.set(action.proto.target.database, new Map());
        }
        const forDatabaseNoSchema = this.byDatabaseAndName.get(action.proto.target.database);
        if (!forDatabaseNoSchema.has(action.proto.target.name)) {
          forDatabaseNoSchema.set(action.proto.target.name, []);
        }
        forDatabaseNoSchema.get(action.proto.target.name).push(action);

        if (!this.byDatabaseSchemaAndName.has(action.proto.target.database)) {
          this.byDatabaseSchemaAndName.set(action.proto.target.database, new Map());
        }
        const forDatabase = this.byDatabaseSchemaAndName.get(action.proto.target.database);
        if (!forDatabase.has(action.proto.target.schema)) {
          forDatabase.set(action.proto.target.schema, new Map());
        }
        const forDatabaseAndSchema = forDatabase.get(action.proto.target.schema);
        if (!forDatabaseAndSchema.has(action.proto.target.name)) {
          forDatabaseAndSchema.set(action.proto.target.name, []);
        }
        forDatabaseAndSchema.get(action.proto.target.name).push(action);
      }
    }
  }

  public find(target: dataform.ITarget) {
    if (!!target.database) {
      if (!!target.schema) {
        return (
          this.byDatabaseSchemaAndName
            .get(this.adapter.normalizeIdentifier(target.database))
            ?.get(this.adapter.normalizeIdentifier(target.schema))
            ?.get(this.adapter.normalizeIdentifier(target.name)) || []
        );
      }
      return (
        this.byDatabaseAndName
          .get(this.adapter.normalizeIdentifier(target.database))
          ?.get(this.adapter.normalizeIdentifier(target.name)) || []
      );
    }
    if (!!target.schema) {
      return (
        this.bySchemaAndName
          .get(this.adapter.normalizeIdentifier(target.schema))
          ?.get(this.adapter.normalizeIdentifier(target.name)) || []
      );
    }
    return this.byName.get(this.adapter.normalizeIdentifier(target.name)) || [];
  }
}
