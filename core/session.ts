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

interface ISqlxConfig {
  type: "view" | "table" | "inline" | "incremental" | "assertion" | "operations";
  dependencies: string[];
  schema?: string;
  name: string;
  hasOutput?: boolean;
  disabled?: boolean;
  redshift?: dataform.IRedshiftOptions;
  bigquery?: dataform.IBigQueryOptions;
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

  public sqlxAction(
    sqlxConfig: ISqlxConfig,
    sqlStatementCount: number,
    hasPreOperations: boolean,
    hasPostOperations: boolean
  ) {
    if (sqlStatementCount > 1 && sqlxConfig.type !== "operations") {
      const message = `Actions may only contain more than one SQL statement if they are of type 'operations'.`;
      this.compileError(new Error(message));
    }
    if (sqlxConfig.hasOutput && sqlxConfig.type !== "operations") {
      const message = `Actions may only specify 'hasOutput: true' if they are of type 'operations'.`;
      this.compileError(new Error(message));
    }
    if (sqlxConfig.hasOutput && sqlStatementCount !== 1) {
      const message = `Operations with 'hasOutput: true' must contain exactly one SQL statement.`;
      this.compileError(new Error(message));
    }
    if (sqlxConfig.disabled && !this.isDatasetType(sqlxConfig.type)) {
      const message = `Actions may only specify 'disabled: true' if they create a dataset.`;
      this.compileError(new Error(message));
    }
    if (sqlxConfig.redshift && !this.isDatasetType(sqlxConfig.type)) {
      const message = `Actions may only specify 'redshift: { ... }' if they create a dataset.`;
      this.compileError(new Error(message));
    }
    if (sqlxConfig.bigquery && !this.isDatasetType(sqlxConfig.type)) {
      const message = `Actions may only specify 'bigquery: { ... }' if they create a dataset.`;
      this.compileError(new Error(message));
    }
    if (hasPreOperations && !this.isDatasetType(sqlxConfig.type)) {
      const message = `Actions may only include pre_operations if they create a dataset.`;
      this.compileError(new Error(message));
    }
    if (hasPostOperations && !this.isDatasetType(sqlxConfig.type)) {
      const message = `Actions may only include post_operations if they create a dataset.`;
      this.compileError(new Error(message));
    }

    const action = (() => {
      switch (sqlxConfig.type) {
        case "view":
        case "table":
        case "inline":
        case "incremental": {
          const dataset = this.publish(sqlxConfig.name);
          dataset.type(sqlxConfig.type);
          if (sqlxConfig.disabled) {
            dataset.disabled();
          }
          if (sqlxConfig.redshift) {
            dataset.redshift(sqlxConfig.redshift);
          }
          if (sqlxConfig.bigquery) {
            dataset.bigquery(sqlxConfig.bigquery);
          }
          return dataset;
        }
        case "assertion": {
          const assertion = this.assert(sqlxConfig.name);
          return assertion;
        }
        case "operations": {
          const operations = this.operate(sqlxConfig.name);
          if (!sqlxConfig.hasOutput) {
            delete operations.proto.target;
          }
          return operations;
        }
        default: {
          throw new Error(`Unrecognized action type: ${sqlxConfig.type}`);
        }
      }
    })();
    if (sqlxConfig.schema) {
      action.proto.target.schema = sqlxConfig.schema;
    }
    if (sqlxConfig.name) {
      action.proto.name = sqlxConfig.name;
      if (action.proto.target) {
        action.proto.target.name = sqlxConfig.name;
      }
    }
    action.dependencies(sqlxConfig.dependencies);
    return action;
  }

  public target(target: string, defaultSchema?: string): dataform.ITarget {
    const suffix = !!this.config.schemaSuffix ? `_${this.config.schemaSuffix}` : "";

    // TODO: this should probably throw, it should be impossible that this codepath gets hit (i think? unless somebody is doing this in JS!)
    if (target.includes(".")) {
      const schema = target.split(".")[0];
      const name = target.split(".")[1];
      return dataform.Target.create({ name, schema: schema + suffix });
    } else {
      return dataform.Target.create({
        name: target,
        schema: (defaultSchema || this.config.defaultSchema) + suffix
      });
    }
  }

  public resolve(name: string): string {
    const table = this.tables[name];
    if (table && table.proto.type === "inline") {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${table.proto.query})`;
    }
    return this.adapter().resolveTarget(this.target(name));
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

  public compileError(err: Error, path?: string) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;

    const compileError = dataform.CompilationError.create({
      stack: err.stack,
      fileName,
      message: err.message
    });
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

    // Expand node dependency wildcards.

    const allNodes: IActionProto[] = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations
    );
    const allNodeNames = allNodes.map(node => node.name);

    allNodes.forEach(node => {
      const uniqueDependencies: { [dependency: string]: boolean } = {};
      const dependencies = node.dependencies || [];
      // Add non-wildcard deps normally.
      dependencies
        .filter(dependency => !dependency.includes("*"))
        .forEach(dependency => (uniqueDependencies[dependency] = true));
      // Match wildcard deps against all node names.
      utils
        .matchPatterns(dependencies.filter(d => d.includes("*")), allNodeNames)
        .forEach(dependency => (uniqueDependencies[dependency] = true));
      node.dependencies = Object.keys(uniqueDependencies);
    });

    return compiledGraph;
  }

  public isDatasetType(type) {
    return type === "view" || type === "table" || type === "inline" || type === "incremental";
  }
}
