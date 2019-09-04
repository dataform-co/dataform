import * as adapters from "@dataform/core/adapters";
import { AConfig, AContextable, Assertion } from "@dataform/core/assertion";
import { DConfig, Declaration } from "@dataform/core/declaration";
import { OConfig, OContextable, Operation } from "@dataform/core/operation";
import * as table from "@dataform/core/table";
import * as test from "@dataform/core/test";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { util } from "protobufjs";

interface IActionProto {
  name?: string;
  fileName?: string;
  dependencies?: string[];
  target?: dataform.ITarget;
}

interface ISqlxConfig extends table.TConfig, AConfig, OConfig, DConfig, test.TConfig {
  type:
    | "view"
    | "table"
    | "inline"
    | "incremental"
    | "assertion"
    | "operations"
    | "declaration"
    | "test";
  name: string;
}

export interface IColumnsDescriptor {
  [name: string]: string | IRecordDescriptor;
}

interface IRecordDescriptor {
  description?: string;
  columns?: IColumnsDescriptor;
}

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
          description: description.description,
          path: currentPath
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

export interface FullyQualifiedName {
  schema: string;
  name: string;
}
export type Resolvable = string | FullyQualifiedName;

export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;

  public actions: Array<table.Table | Operation | Assertion | Declaration>;
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
    return adapters.create(this.config);
  }

  public sqlxAction(actionOptions: {
    sqlxConfig: ISqlxConfig;
    sqlStatementCount: number;
    hasIncremental: boolean;
    hasPreOperations: boolean;
    hasPostOperations: boolean;
    hasInputs: boolean;
  }) {
    if (actionOptions.sqlStatementCount > 1 && actionOptions.sqlxConfig.type !== "operations") {
      this.compileError(
        "Actions may only contain more than one SQL statement if they are of type 'operations'."
      );
    }
    if (
      actionOptions.sqlxConfig.hasOutput &&
      !(
        actionOptions.sqlxConfig.type === "operations" ||
        definesDataset(actionOptions.sqlxConfig.type)
      )
    ) {
      this.compileError(
        "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
      );
    }
    if (
      actionOptions.sqlxConfig.columns &&
      !declaresDataset(actionOptions.sqlxConfig.type, actionOptions.sqlxConfig.hasOutput)
    ) {
      this.compileError("Actions may only specify 'columns' if they create or declare a dataset.");
    }
    if (actionOptions.sqlxConfig.protected && actionOptions.sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only specify 'protected: true' if they are of type 'incremental'."
      );
    }
    if (actionOptions.hasIncremental && actionOptions.sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only include incremental_where if they are of type 'incremental'."
      );
    }
    if (!actionOptions.sqlxConfig.schema && actionOptions.sqlxConfig.type === "declaration") {
      this.compileError("Actions of type 'declaration' must specify a value for 'schema'.");
    }
    if (actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only specify 'dataset' if they are of type 'test'.");
    }
    if (!actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type === "test") {
      this.compileError("Actions must specify 'dataset' if they are of type 'test'.");
    }
    if (actionOptions.hasInputs && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (actionOptions.sqlxConfig.disabled && !definesDataset(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'disabled: true' if they create a dataset.");
    }
    if (actionOptions.sqlxConfig.redshift && !definesDataset(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'redshift: { ... }' if they create a dataset.");
    }
    if (
      actionOptions.sqlxConfig.sqldatawarehouse &&
      !definesDataset(actionOptions.sqlxConfig.type)
    ) {
      this.compileError(
        "Actions may only specify 'sqldatawarehouse: { ... }' if they create a dataset."
      );
    }
    if (actionOptions.sqlxConfig.bigquery && !definesDataset(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'bigquery: { ... }' if they create a dataset.");
    }
    if (actionOptions.hasPreOperations && !definesDataset(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.hasPostOperations && !definesDataset(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include post_operations if they create a dataset.");
    }

    const action = (() => {
      switch (actionOptions.sqlxConfig.type) {
        case "view":
        case "table":
        case "inline":
        case "incremental":
          return this.publish(actionOptions.sqlxConfig.name);
        case "assertion":
          return this.assert(actionOptions.sqlxConfig.name);
        case "operations":
          return this.operate(actionOptions.sqlxConfig.name);
        case "declaration":
          return this.declare({
            schema: actionOptions.sqlxConfig.schema,
            name: actionOptions.sqlxConfig.name
          });
        case "test":
          return this.test(actionOptions.sqlxConfig.name);
        default:
          throw new Error(`Unrecognized action type: ${actionOptions.sqlxConfig.type}`);
      }
    })().config(actionOptions.sqlxConfig);
    return action;
  }

  public resolve(ref: Resolvable): string {
    const allResolved = this.findActions(ref);
    if (allResolved.length > 1) {
      this.compileError(new Error(utils.ambiguousActionNameMsg(ref, allResolved)));
    }
    const resolved = allResolved.length > 0 ? allResolved[0] : undefined;

    if (resolved && resolved instanceof table.Table && resolved.proto.type === "inline") {
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
        this.target(ref, `${this.config.defaultSchema}${this.getSuffixWithUnderscore()}`)
      );
    }
    return this.adapter().resolveTarget(
      this.target(ref.name, `${ref.schema}${this.getSuffixWithUnderscore()}`)
    );
  }

  public operate(name: string, queries?: OContextable<string | string[]>): Operation {
    const operation = new Operation();
    operation.session = this;
    this.setNameAndTarget(operation.proto, name);
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(operation);
    return operation;
  }

  public publish(
    name: string,
    queryOrConfig?: table.TContextable<string> | table.TConfig
  ): table.Table {
    const newTable = new table.Table();
    newTable.session = this;
    this.setNameAndTarget(newTable.proto, name);
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
    this.setNameAndTarget(assertion.proto, name, this.config.assertionSchema);
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(assertion);
    return assertion;
  }

  public declare(dataset: FullyQualifiedName): Declaration {
    const declaration = new Declaration();
    declaration.session = this;
    this.setNameAndTarget(declaration.proto, dataset.name, dataset.schema);
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

  public compile(): dataform.ICompiledGraph {
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.actions.filter(action => action instanceof table.Table)),
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
      graphErrors: this.graphErrors
    });

    this.fullyQualifyDependencies(
      [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
    );

    if (!!this.config.schemaSuffix) {
      this.appendSchemaSuffix(
        [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations)
      );
    }

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

  public findActions(res: Resolvable) {
    const adapter = this.adapter();
    return this.actions.filter(action => {
      if (typeof res === "string") {
        return action.proto.target.name === adapter.normalizeIdentifier(res);
      }
      return (
        action.proto.target.schema === adapter.normalizeIdentifier(res.schema) &&
        action.proto.target.name === adapter.normalizeIdentifier(res.name)
      );
    });
  }

  public setNameAndTarget(action: IActionProto, name: string, overrideSchema?: string) {
    const newTarget = this.target(name, overrideSchema || this.config.defaultSchema);
    action.target = newTarget;
    action.name = `${action.target.schema}.${action.target.name}`;
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

  private target(name: string, schema: string): dataform.ITarget {
    const adapter = this.adapter();
    return dataform.Target.create({
      name: adapter.normalizeIdentifier(name),
      schema: adapter.normalizeIdentifier(schema)
    });
  }

  private fullyQualifyDependencies(actions: IActionProto[]) {
    const allActionsByName = keyByName(actions);
    actions.forEach(action => {
      action.dependencies = (action.dependencies || []).map(dependencyName => {
        if (!!allActionsByName[dependencyName]) {
          // Dependency is already fully-qualified.
          return dependencyName;
        }
        const possibleDeps = this.findActions(dependencyName);
        if (possibleDeps.length === 1) {
          return possibleDeps[0].proto.name;
        }
        if (possibleDeps.length >= 1) {
          this.compileError(new Error(utils.ambiguousActionNameMsg(dependencyName, possibleDeps)));
        } else {
          this.compileError(
            new Error(
              `Missing dependency detected: Action "${action.name}" depends on "${dependencyName}" which does not exist.`
            ),
            action.fileName
          );
        }
        return dependencyName;
      });
    });
  }

  private appendSchemaSuffix(actions: IActionProto[]) {
    const suffixedNames: { [originalName: string]: string } = {};
    actions.forEach(action => {
      const originalName = action.name;
      action.target = {
        ...action.target,
        schema: `${action.target.schema}${this.getSuffixWithUnderscore()}`
      };
      action.name = `${action.target.schema}.${action.target.name}`;
      suffixedNames[originalName] = action.name;
    });

    // Fix up dependencies in case those dependencies' names have changed.
    actions.forEach(action => {
      action.dependencies = (action.dependencies || []).map(
        dependencyName => suffixedNames[dependencyName] || dependencyName
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
    const checkCircular = (action: IActionProto, dependents: IActionProto[]): boolean => {
      if (dependents.indexOf(action) >= 0) {
        const message = `Circular dependency detected in chain: [${dependents
          .map(d => d.name)
          .join(" > ")} > ${action.name}]`;
        this.compileError(new Error(message), action.fileName);
        return true;
      }
      return (action.dependencies || []).some(
        dependencyName =>
          allActionsByName[dependencyName] &&
          checkCircular(allActionsByName[dependencyName], dependents.concat([action]))
      );
    };

    for (const action of actions) {
      if (checkCircular(action, [])) {
        break;
      }
    }
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
