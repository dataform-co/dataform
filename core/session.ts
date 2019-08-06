import * as adapters from "@dataform/core/adapters";
import { AContextable, Assertion } from "@dataform/core/assertion";
import { OContextable, Operation } from "@dataform/core/operation";
import { Table, TConfig, TContextable } from "@dataform/core/table";
import { Test } from "@dataform/core/test";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

interface IActionProto {
  name?: string;
  fileName?: string;
  dependencies?: string[];
}

interface ISqlxConfig extends TConfig {
  type: "view" | "table" | "inline" | "incremental" | "assertion" | "operations" | "test";
  schema?: string;
  name: string;
  hasOutput?: boolean;
  dataset?: string;
  tags?: string[];
}

export interface IResolvable {
  schema: string;
  name: string;
}

export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;

  public tables: { [name: string]: Table };
  public operations: { [name: string]: Operation };
  public assertions: { [name: string]: Assertion };
  public tests: { [name: string]: Test };

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
    this.tables = {};
    this.operations = {};
    this.assertions = {};
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
      (actionOptions.sqlxConfig.type !== "operations" ||
        this.isDatasetType(actionOptions.sqlxConfig.type))
    ) {
      this.compileError(
        "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
      );
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
    if (actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only specify 'dataset' if they are of type 'test'.");
    }
    if (!actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type === "test") {
      this.compileError("Actions must specify 'dataset' if they are of type 'test'.");
    }
    if (actionOptions.hasInputs && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (actionOptions.sqlxConfig.disabled && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'disabled: true' if they create a dataset.");
    }
    if (actionOptions.sqlxConfig.redshift && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'redshift: { ... }' if they create a dataset.");
    }
    if (actionOptions.sqlxConfig.bigquery && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'bigquery: { ... }' if they create a dataset.");
    }
    if (actionOptions.hasPreOperations && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.hasPostOperations && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include post_operations if they create a dataset.");
    }

    if (actionOptions.sqlxConfig.type === "test") {
      return this.test(actionOptions.sqlxConfig.name).dataset(actionOptions.sqlxConfig.dataset);
    }

    const action = (() => {
      switch (actionOptions.sqlxConfig.type) {
        case "view":
        case "table":
        case "inline":
        case "incremental": {
          const dataset = this.publish(actionOptions.sqlxConfig.name);
          dataset.config(actionOptions.sqlxConfig);
          return dataset;
        }
        case "assertion": {
          const assertion = this.assert(actionOptions.sqlxConfig.name);
          assertion.dependencies(actionOptions.sqlxConfig.dependencies);
          assertion.tags(actionOptions.sqlxConfig.tags);
          return assertion;
        }
        case "operations": {
          const operations = this.operate(actionOptions.sqlxConfig.name);
          if (!actionOptions.sqlxConfig.hasOutput) {
            delete operations.proto.target;
          }
          operations.dependencies(actionOptions.sqlxConfig.dependencies);
          operations.tags(actionOptions.sqlxConfig.tags);
          return operations;
        }
        default: {
          throw new Error(`Unrecognized action type: ${actionOptions.sqlxConfig.type}`);
        }
      }
    })();
    if (action.proto.target) {
      const finalSchema =
        actionOptions.sqlxConfig.schema ||
        (actionOptions.sqlxConfig.type === "assertion"
          ? this.config.assertionSchema
          : this.config.defaultSchema);
      action.proto.target = this.target(actionOptions.sqlxConfig.name, finalSchema);
    }
    return action;
  }

  public target(target: string, defaultSchema?: string): dataform.ITarget {
    const suffix = !!this.config.schemaSuffix ? `_${this.config.schemaSuffix}` : "";

    if (target.includes(".")) {
      const [schema, name] = target.split(".");
      return dataform.Target.create({ name, schema: schema + suffix });
    }
    return dataform.Target.create({
      name: target,
      schema: (defaultSchema || this.config.defaultSchema) + suffix
    });
  }

  public resolve(reference: string | IResolvable): string {
    let [fQName, err] = ["", ""];
    if (typeof reference === "string") {
      [fQName, err] = utils.matchFQName(reference, this.getAllFQNames());
      if (err) {
        this.compileError(new Error(err));
      }
    } else {
      fQName = (reference as IResolvable).schema + "." + (reference as IResolvable).name;
    }
    const table = this.tables[fQName];
    const operation =
      !!this.operations[fQName] && this.operations[fQName].hasOutput && this.operations[fQName];

    if (table && table.proto.type === "inline") {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${table.proto.query})`;
    }

    const dataset = table || operation;
    // TODO: We fall back to using the plain 'name' here for backwards compatibility with projects that use .sql files.
    // In these projects, this session may not know about all actions (yet), and thus we need to fall back to assuming
    // that the target *will* exist in the future. Once we break backwards compatibility with .sql files, we should remove
    // the code that calls 'this.target(...)' below, and append a compile error if we can't find a dataset whose name is 'name'.
    const target = dataset ? dataset.proto.target : this.target(name);
    return this.adapter().resolveTarget(target);
  }

  public operate(name: string, queries?: OContextable<string | string[]>): Operation {
    const operation = new Operation();
    operation.session = this;
    operation.proto.target = this.target(name);
    const fQName = operation.proto.target.schema + "." + operation.proto.target.name;
    this.checkActionNameIsUnused(fQName);
    operation.proto.name = fQName;
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.operations[fQName] = operation;
    return operation;
  }

  public publish(name: string, queryOrConfig?: TContextable<string> | TConfig): Table {
    const table = new Table();
    table.session = this;
    table.proto.target = this.target(name);
    const fQName = table.proto.target.schema + "." + table.proto.target.name;
    this.checkActionNameIsUnused(fQName);
    table.proto.name = fQName;
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        table.config(queryOrConfig);
      } else {
        table.query(queryOrConfig);
      }
    }
    table.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tables[fQName] = table;
    return table;
  }

  public assert(name: string, query?: AContextable<string>): Assertion {
    const assertion = new Assertion();
    assertion.session = this;
    assertion.proto.target = this.target(name, this.config.assertionSchema);
    const fQName = assertion.proto.target.schema + "." + assertion.proto.target.name;
    this.checkActionNameIsUnused(fQName);
    assertion.proto.name = fQName;
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.assertions[fQName] = assertion;
    return assertion;
  }

  public test(name: string): Test {
    this.checkTestNameIsUnused(name);
    const test = new Test();
    test.session = this;
    test.proto.name = name;
    test.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tests[name] = test;
    return test;
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

  public compileGraphChunk<T>(part: {
    [name: string]: { proto: IActionProto; compile(): T };
  }): T[] {
    const compiledChunks: T[] = [];

    Object.keys(part).forEach(key => {
      try {
        const compiledChunk = part[key].compile();
        compiledChunks.push(compiledChunk);
      } catch (e) {
        this.compileError(e, part[key].proto.fileName);
      }
    });

    return compiledChunks;
  }

  public compile(): dataform.ICompiledGraph {
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.tables),
      operations: this.compileGraphChunk(this.operations),
      assertions: this.compileGraphChunk(this.assertions),
      tests: this.compileGraphChunk(this.tests),
      graphErrors: this.graphErrors
    });

    const allActions: IActionProto[] = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations
    );
    const allActionNames = allActions.map(action => action.name);

    allActions.forEach(action => {
      const uniqueDependencies: { [dependency: string]: boolean } = {};
      const dependencies = action.dependencies || [];
      dependencies.forEach(dependency => (uniqueDependencies[dependency] = true));
      action.dependencies = Object.keys(uniqueDependencies);
    });

    return compiledGraph;
  }

  public isDatasetType(type) {
    return type === "view" || type === "table" || type === "inline" || type === "incremental";
  }

  public getAllFQNames() {
    return [].concat(
      Object.keys(this.tables),
      Object.keys(this.assertions),
      Object.keys(this.operations)
    );
  }

  private checkActionNameIsUnused(name: string) {
    // Check for duplicate names
    if (this.tables[name] || this.operations[name] || this.assertions[name]) {
      const message = `Duplicate action name detected. Names within a schema must be unique across tables, assertions, and operations: "${name}"`;
      this.compileError(new Error(message));
    }
  }

  private checkTestNameIsUnused(name: string) {
    // Check for duplicate names
    if (this.tests[name]) {
      const message = `Duplicate test name detected: "${name}"`;
      this.compileError(new Error(message));
    }
  }
}
