import * as adapters from "@dataform/core/adapters";
import { AContextable, Assertion } from "@dataform/core/assertion";
import { OContextable, Operation } from "@dataform/core/operation";
import { Table, TConfig, TContextable } from "@dataform/core/table";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

interface IActionProto {
  name?: string;
  fileName?: string;
  dependencies?: string[];
}

interface ISqlxConfig extends TConfig {
  type: "view" | "table" | "inline" | "incremental" | "assertion" | "operations";
  schema?: string;
  name: string;
  hasOutput?: boolean;
}

export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;

  public tables: { [name: string]: Table };
  public operations: { [name: string]: Operation };
  public assertions: { [name: string]: Assertion };

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
    if (actionOptions.sqlxConfig.hasOutput && actionOptions.sqlStatementCount !== 1) {
      this.compileError(
        "Operations with 'hasOutput: true' must contain exactly one SQL statement."
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
          return assertion;
        }
        case "operations": {
          const operations = this.operate(actionOptions.sqlxConfig.name);
          if (!actionOptions.sqlxConfig.hasOutput) {
            delete operations.proto.target;
          }
          operations.dependencies(actionOptions.sqlxConfig.dependencies);
          return operations;
        }
        default: {
          throw new Error(`Unrecognized action type: ${actionOptions.sqlxConfig.type}`);
        }
      }
    })();
    if (actionOptions.sqlxConfig.schema) {
      action.proto.target.schema = actionOptions.sqlxConfig.schema;
    }
    if (actionOptions.sqlxConfig.name) {
      action.proto.name = actionOptions.sqlxConfig.name;
      if (action.proto.target) {
        action.proto.target.name = actionOptions.sqlxConfig.name;
      }
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

  public resolve(name: string): string {
    const table = this.tables[name];
    const operation = this.operations[name];
    if (!table && !operation && !operation.hasOutput) {
      this.compileError(new Error(`Unrecognized dataset name: "${name}".`));
      return "";
    }
    if (table && table.proto.type === "inline") {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${table.proto.query})`;
    }
    const dataset = table || operation;
    return this.adapter().resolveTarget(dataset.proto.target);
  }

  public operate(name: string, queries?: OContextable<string | string[]>): Operation {
    const operation = new Operation();
    operation.session = this;
    operation.proto.name = name;
    operation.proto.target = this.target(name);
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.operations[name] = operation;
    return operation;
  }

  public publish(name: string, queryOrConfig?: TContextable<string> | TConfig): Table {
    // Check for duplicate names
    if (this.tables[name]) {
      const message = `Duplicate action name detected, names must be unique across tables, assertions, and operations: "${name}"`;
      this.compileError(new Error(message));
    }

    const table = new Table();
    table.session = this;
    table.proto.name = name;
    table.proto.target = this.target(name);
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        table.config(queryOrConfig);
      } else {
        table.query(queryOrConfig);
      }
    }
    table.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tables[name] = table;
    return table;
  }

  public assert(name: string, query?: AContextable<string>): Assertion {
    const assertion = new Assertion();
    assertion.session = this;
    assertion.proto.name = name;
    assertion.proto.target = this.target(name, this.config.assertionSchema);
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.assertions[name] = assertion;
    return assertion;
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
      graphErrors: this.graphErrors
    });

    // Expand action dependency wildcards.

    const allActions: IActionProto[] = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations
    );
    const allActionNames = allActions.map(action => action.name);

    allActions.forEach(action => {
      const uniqueDependencies: { [dependency: string]: boolean } = {};
      const dependencies = action.dependencies || [];
      // Add non-wildcard deps normally.
      dependencies
        .filter(dependency => !dependency.includes("*"))
        .forEach(dependency => (uniqueDependencies[dependency] = true));
      // Match wildcard deps against all action names.
      utils
        .matchPatterns(dependencies.filter(d => d.includes("*")), allActionNames)
        .forEach(dependency => (uniqueDependencies[dependency] = true));
      action.dependencies = Object.keys(uniqueDependencies);
    });

    return compiledGraph;
  }

  public isDatasetType(type) {
    return type === "view" || type === "table" || type === "inline" || type === "incremental";
  }
}
