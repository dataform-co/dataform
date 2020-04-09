import * as adapters from "@dataform/core/adapters";
import { AContextable, Assertion, IAssertionConfig } from "@dataform/core/assertion";
import {
  IColumnsDescriptor,
  ICommonContext,
  IRecordDescriptor,
  Resolvable
} from "@dataform/core/common";
import { Contextable } from "@dataform/core/common";
import { Declaration, IDeclarationConfig } from "@dataform/core/declaration";
import { IOperationConfig, Operation } from "@dataform/core/operation";
import { ITableConfig, ITableContext, Table, TableType } from "@dataform/core/table";
import * as test from "@dataform/core/test";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { util } from "protobufjs";
import { Graph as TarjanGraph } from "tarjan-graph";
import * as TarjanGraphConstructor from "tarjan-graph";

// Can't use resolveJsonModule with Bazel.
// tslint:disable-next-line: no-var-requires
const { version: dataformCoreVersion } = require("@dataform/core/package.json");

/**
 * @hidden
 */
export interface IActionProto {
  name?: string;
  fileName?: string;
  dependencyTargets?: dataform.ITarget[];
  dependencies?: string[];
  target?: dataform.ITarget;
}

type SqlxConfig = (
  | ITableConfig & { type: TableType }
  | IAssertionConfig & { type: "assertion" }
  | IOperationConfig & { type: "operations" }
  | IDeclarationConfig & { type: "declaration" }
  | test.ITestConfig & { type: "test" }) & { name: string };

/**
 * @hidden
 */
export function mapToColumnProtoArray(columns: IColumnsDescriptor): dataform.IColumnDescriptor[] {
  return utils.flatten(
    Object.keys(columns).map(column => mapColumnDescriptionToProto([column], columns[column]))
  );
}

function mapColumnDescriptionToProto(
  currentPath: string[],
  description: string | IRecordDescriptor
): dataform.IColumnDescriptor[] {
  if (typeof description === "string") {
    return [
      dataform.ColumnDescriptor.create({
        description,
        path: currentPath
      })
    ];
  }
  const columnDescriptor: dataform.IColumnDescriptor[] = description.description
    ? [
        dataform.ColumnDescriptor.create({
          path: currentPath,
          description: description.description,
          displayName: description.displayName,
          dimensionType: mapDimensionType(description.dimension),
          aggregation: mapAggregation(description.aggregation),
          expression: description.expression
        })
      ]
    : [];
  const nestedColumns = description.columns ? Object.keys(description.columns) : [];
  return columnDescriptor.concat(
    utils.flatten(
      nestedColumns.map(nestedColumn =>
        mapColumnDescriptionToProto(
          currentPath.concat([nestedColumn]),
          description.columns[nestedColumn]
        )
      )
    )
  );
}

function mapAggregation(aggregation: string) {
  switch (aggregation) {
    case "sum":
      return dataform.ColumnDescriptor.Aggregation.SUM;
    case "distinct":
      return dataform.ColumnDescriptor.Aggregation.DISTINCT;
    case "derived":
      return dataform.ColumnDescriptor.Aggregation.DERIVED;
    default:
      return null;
  }
}
function mapDimensionType(dimensionType: string) {
  switch (dimensionType) {
    case "category":
      return dataform.ColumnDescriptor.DimensionType.CATEGORY;
    case "timestamp":
      return dataform.ColumnDescriptor.DimensionType.TIMESTAMP;
    default:
      return null;
  }
}

/**
 * @hidden
 */
export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;

  public actions: Array<Table | Operation | Assertion | Declaration>;
  public tests: { [name: string]: test.Test };

  public graphErrors: dataform.IGraphErrors;

  constructor(rootDir: string, projectConfig?: dataform.IProjectConfig) {
    this.init(rootDir, projectConfig);
  }

  public init(rootDir: string, projectConfig?: dataform.IProjectConfig) {
    this.rootDir = rootDir;
    this.config = projectConfig || {
      defaultSchema: "dataform",
      assertionSchema: "dataform_assertions"
    };
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
        schema: `${resolved.proto.target.schema}${this.getSuffixWithUnderscore()}`
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
          ref,
          `${this.config.defaultSchema}${this.getSuffixWithUnderscore()}`
        )
      );
    }
    return this.adapter().resolveTarget(
      utils.target(this.adapter(), ref.name, `${ref.schema}${this.getSuffixWithUnderscore()}`)
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
