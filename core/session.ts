import * as adapters from "df/core/adapters";
import { AContextable, Assertion, IAssertionConfig } from "df/core/assertion";
import { Contextable, ICommonContext, Resolvable } from "df/core/common";
import { Declaration, IDeclarationConfig } from "df/core/declaration";
import { IOperationConfig, Operation } from "df/core/operation";
import { ITableConfig, ITableContext, Table, TableType } from "df/core/table";
import * as test from "df/core/test";
import * as utils from "df/core/utils";
import { version as dataformCoreVersion } from "df/core/version";
import { dataform } from "df/protos/ts";
import { util } from "protobufjs";
import { default as TarjanGraphConstructor, Graph as TarjanGraph } from "tarjan-graph";

const DEFAULT_CONFIG = {
  defaultSchema: "dataform",
  assertionSchema: "dataform_assertions"
};

/**
 * @hidden
 */
export interface IActionProto {
  name?: string;
  fileName?: string;
  dependencyTargets?: dataform.ITarget[];
  dependencies?: string[];
  hermeticity?: dataform.ActionHermeticity;
  target?: dataform.ITarget;
  canonicalTarget?: dataform.ITarget;
}

type SqlxConfig = (
  | (ITableConfig & { type: TableType })
  | (IAssertionConfig & { type: "assertion" })
  | (IOperationConfig & { type: "operations" })
  | (IDeclarationConfig & { type: "declaration" })
  | (test.ITestConfig & { type: "test" })
) & { name: string };

/**
 * @hidden
 */
export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;
  public canonicalConfig: dataform.IProjectConfig;

  public actions: Array<Table | Operation | Assertion | Declaration>;
  public tests: { [name: string]: test.Test };

  public graphErrors: dataform.IGraphErrors;

  constructor(
    rootDir: string,
    projectConfig?: dataform.IProjectConfig,
    originalProjectConfig?: dataform.IProjectConfig
  ) {
    this.init(rootDir, projectConfig, originalProjectConfig);
  }

  public init(
    rootDir: string,
    projectConfig?: dataform.IProjectConfig,
    originalProjectConfig?: dataform.IProjectConfig
  ) {
    this.rootDir = rootDir;
    this.config = projectConfig || DEFAULT_CONFIG;
    this.canonicalConfig = getCanonicalProjectConfig(
      originalProjectConfig || projectConfig || DEFAULT_CONFIG
    );
    this.actions = [];
    this.tests = {};
    this.graphErrors = { compilationErrors: [] };
  }

  public adapter(): adapters.IAdapter {
    return adapters.create(this.config, dataformCoreVersion);
  }

  public sqlxAction(actionOptions: {
    sqlxConfig: SqlxConfig;
    sqlStatementCount: number;
    hasIncremental: boolean;
    hasPreOperations: boolean;
    hasPostOperations: boolean;
    hasInputs: boolean;
  }) {
    const { sqlxConfig } = actionOptions;
    if (actionOptions.sqlStatementCount > 1 && sqlxConfig.type !== "operations") {
      this.compileError(
        "Actions may only contain more than one SQL statement if they are of type 'operations'."
      );
    }
    if (
      sqlxConfig.hasOwnProperty("hasOutput") &&
      !(sqlxConfig.type === "operations" || definesDataset(sqlxConfig.type))
    ) {
      this.compileError(
        "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
      );
    }
    if (
      sqlxConfig.hasOwnProperty("columns") &&
      !declaresDataset(sqlxConfig.type, sqlxConfig.hasOwnProperty("hasOutput"))
    ) {
      this.compileError("Actions may only specify 'columns' if they create or declare a dataset.");
    }
    if (sqlxConfig.hasOwnProperty("protected") && sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only specify 'protected: true' if they are of type 'incremental'."
      );
    }
    if (actionOptions.hasIncremental && sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only include incremental_where if they are of type 'incremental'."
      );
    }
    if (!sqlxConfig.hasOwnProperty("schema") && sqlxConfig.type === "declaration") {
      this.compileError("Actions of type 'declaration' must specify a value for 'schema'.");
    }
    if (sqlxConfig.hasOwnProperty("dataset") && sqlxConfig.type !== "test") {
      this.compileError("Actions may only specify 'dataset' if they are of type 'test'.");
    }
    if (!sqlxConfig.hasOwnProperty("dataset") && sqlxConfig.type === "test") {
      this.compileError("Actions must specify 'dataset' if they are of type 'test'.");
    }
    if (actionOptions.hasInputs && sqlxConfig.type !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (sqlxConfig.hasOwnProperty("disabled") && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only specify 'disabled: true' if they create a dataset.");
    }
    if (sqlxConfig.hasOwnProperty("redshift") && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only specify 'redshift: { ... }' if they create a dataset.");
    }
    if (sqlxConfig.hasOwnProperty("sqldatawarehouse") && !definesDataset(sqlxConfig.type)) {
      this.compileError(
        "Actions may only specify 'sqldatawarehouse: { ... }' if they create a dataset."
      );
    }
    if (sqlxConfig.hasOwnProperty("bigquery") && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only specify 'bigquery: { ... }' if they create a dataset.");
    }
    if (actionOptions.hasPreOperations && !definesDataset(sqlxConfig.type)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.hasPostOperations && !definesDataset(sqlxConfig.type)) {
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

    const action = (() => {
      switch (sqlxConfig.type) {
        case "view":
        case "table":
        case "inline":
        case "incremental":
          return this.publish(sqlxConfig.name).config(sqlxConfig);
        case "assertion":
          return this.assert(sqlxConfig.name).config(sqlxConfig);
        case "operations":
          return this.operate(sqlxConfig.name).config(sqlxConfig);
        case "declaration":
          return this.declare({
            database: sqlxConfig.database,
            schema: sqlxConfig.schema,
            name: sqlxConfig.name
          }).config(sqlxConfig);
        case "test":
          return this.test(sqlxConfig.name).config(sqlxConfig);
        default:
          throw new Error(`Unrecognized action type: ${(sqlxConfig as SqlxConfig).type}`);
      }
    })();
    return action;
  }

  public resolve(ref: Resolvable): string {
    const allResolved = this.findActions(utils.resolvableAsTarget(ref));
    if (allResolved.length > 1) {
      this.compileError(new Error(utils.ambiguousActionNameMsg(ref, allResolved)));
    }
    const resolved = allResolved.length > 0 ? allResolved[0] : undefined;

    if (resolved && resolved instanceof Table && resolved.proto.type === "inline") {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${resolved.proto.query})`;
    }
    if (resolved && resolved instanceof Operation && !resolved.proto.hasOutput) {
      this.compileError(
        new Error("Actions cannot resolve operations which do not produce output.")
      );
    }

    if (resolved) {
      if (resolved instanceof Declaration) {
        return this.adapter().resolveTarget(resolved.proto.target);
      }
      return this.adapter().resolveTarget({
        ...resolved.proto.target,
        schema: `${resolved.proto.target.schema}${this.getSuffixWithUnderscore()}`,
        name: `${this.getTablePrefixWithUnderscore()}${resolved.proto.target.name}`
      });
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
          `${this.getTablePrefixWithUnderscore()}${ref}`,
          `${this.config.defaultSchema}${this.getSuffixWithUnderscore()}`
        )
      );
    }
    return this.adapter().resolveTarget(
      utils.target(
        this.adapter(),
        this.config,
        `${this.getTablePrefixWithUnderscore()}${ref.name}`,
        `${ref.schema}${this.getSuffixWithUnderscore()}`
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

  public compileError(err: Error | string, path?: string) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;

    const compileError = dataform.CompilationError.create({
      fileName
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
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.actions.filter(action => action instanceof Table)),
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
      graphErrors: this.graphErrors,
      dataformCoreVersion,
      targets: this.actions.map(action => action.proto.target)
    });

    this.fullyQualifyDependencies(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
    );

    this.alterActionName(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
    );

    this.checkActionNameUniqueness(
      [].concat(
        compiledGraph.tables,
        compiledGraph.assertions,
        compiledGraph.operations,
        compiledGraph.declarations
      )
    );
    this.checkTestNameUniqueness(compiledGraph.tests);

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

    return compiledGraph;
  }

  public compileToBase64() {
    const encodedGraphBytes = dataform.CompiledGraph.encode(this.compile()).finish();
    return util.base64.encode(encodedGraphBytes, 0, encodedGraphBytes.length);
  }

  public findActions(target: dataform.ITarget) {
    const adapter = this.adapter();
    return this.actions.filter(action => {
      if (
        !!target.database &&
        action.proto.target.database !== adapter.normalizeIdentifier(target.database)
      ) {
        return false;
      }
      if (
        !!target.schema &&
        action.proto.target.schema !== adapter.normalizeIdentifier(target.schema)
      ) {
        return false;
      }
      return action.proto.target.name === adapter.normalizeIdentifier(target.name);
    });
  }

  private getSuffixWithUnderscore() {
    return !!this.config.schemaSuffix ? `_${this.config.schemaSuffix}` : "";
  }

  private compileGraphChunk<T>(actions: Array<{ proto: IActionProto; compile(): T }>): T[] {
    const compiledChunks: T[] = [];

    actions.forEach(action => {
      try {
        const compiledChunk = action.compile();
        compiledChunks.push(compiledChunk);
      } catch (e) {
        this.compileError(e, action.proto.fileName);
      }
    });

    return compiledChunks;
  }

  private fullyQualifyDependencies(actions: IActionProto[]) {
    actions.forEach(action => {
      const fullyQualifiedDependencies: { [name: string]: dataform.ITarget } = {};
      for (const dependency of action.dependencyTargets) {
        const possibleDeps = this.findActions(dependency);
        if (possibleDeps.length === 0) {
          // We couldn't find a matching target.
          this.compileError(
            new Error(
              `Missing dependency detected: Action "${
                action.name
              }" depends on "${utils.stringifyResolvable(dependency)}" which does not exist.`
            ),
            action.fileName
          );
        } else if (possibleDeps.length === 1) {
          // We found a single matching target, and fully-qualify it if it's a normal dependency,
          // or add all of its dependencies to ours if it's an 'inline' table.
          const protoDep = possibleDeps[0].proto;
          if (protoDep instanceof dataform.Table && protoDep.type === "inline") {
            protoDep.dependencyTargets.forEach(inlineDep =>
              action.dependencyTargets.push(inlineDep)
            );
          } else {
            fullyQualifiedDependencies[protoDep.name] = protoDep.target;
          }
        } else {
          // Too many targets matched the dependency.
          this.compileError(new Error(utils.ambiguousActionNameMsg(dependency, possibleDeps)));
        }
      }
      action.dependencies = Object.keys(fullyQualifiedDependencies);
      action.dependencyTargets = Object.values(fullyQualifiedDependencies);
    });
  }

  private getTablePrefixWithUnderscore() {
    return !!this.config.tablePrefix ? `${this.config.tablePrefix}_` : "";
  }

  private alterActionName(actions: IActionProto[]) {
    const { tablePrefix, schemaSuffix } = this.config;

    if (!tablePrefix && !schemaSuffix) {
      return;
    }

    const actionNames: { [originalName: string]: string } = {};

    actions.forEach(action => {
      const originalName = action.name;
      action.target = {
        ...action.target,
        name: `${this.getTablePrefixWithUnderscore()}${action.target.name}`,
        schema: `${action.target.schema}${this.getSuffixWithUnderscore()}`
      };
      action.name = `${!!action.target.database ? `${action.target.database}.` : ""}${
        action.target.schema
      }.${action.target.name}`;
      actionNames[originalName] = action.name;
    });

    // Fix up dependencies in case those dependencies' names have changed.
    actions.forEach(action => {
      action.dependencies = (action.dependencies || []).map(
        dependencyName => actionNames[dependencyName] || dependencyName
      );
    });
  }

  private checkActionNameUniqueness(actions: IActionProto[]) {
    const allNames: string[] = [];
    actions.forEach(action => {
      if (allNames.includes(action.name)) {
        this.compileError(
          new Error(
            `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations: "${action.name}"`
          ),
          action.fileName
        );
      }
      allNames.push(action.name);
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
    const allActionsByName = keyByName(actions);

    // Type exports for tarjan-graph are unfortunately wrong, so we have to do this minor hack.
    const tarjanGraph: TarjanGraph = new (TarjanGraphConstructor as any)();
    actions.forEach(action => {
      const cleanedDependencies = (action.dependencies || []).filter(
        dependency => !!allActionsByName[dependency]
      );
      tarjanGraph.add(action.name, cleanedDependencies);
    });
    const cycles = tarjanGraph.getCycles();
    cycles.forEach(cycle => {
      const firstActionInCycle = allActionsByName[cycle[0].name];
      const message = `Circular dependency detected in chain: [${cycle
        .map(vertex => vertex.name)
        .join(" > ")} > ${firstActionInCycle.name}]`;
      this.compileError(new Error(message), firstActionInCycle.fileName);
    });
  }

  private checkRunCachingCorrectness(actionsWithOutput: IActionProto[]) {
    actionsWithOutput.forEach(action => {
      if (action.dependencies?.length > 0) {
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
        action.fileName
      );
    });
  }
}

function declaresDataset(type: string, hasOutput?: boolean) {
  return definesDataset(type) || type === "declaration" || hasOutput;
}

function definesDataset(type: string) {
  return type === "view" || type === "table" || type === "inline" || type === "incremental";
}

function keyByName(actions: IActionProto[]) {
  const actionsByName: { [name: string]: IActionProto } = {};
  actions.forEach(action => (actionsByName[action.name] = action));
  return actionsByName;
}

function getCanonicalProjectConfig(originalProjectConfig: dataform.IProjectConfig) {
  return {
    warehouse: originalProjectConfig.warehouse,
    defaultSchema: originalProjectConfig.defaultSchema,
    defaultDatabase: originalProjectConfig.defaultDatabase,
    assertionSchema: originalProjectConfig.assertionSchema
  };
}
