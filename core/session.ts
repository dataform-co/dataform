import * as protos from "@dataform/protos";
import * as adapters from "./adapters";
import * as utils from "./utils";
import { Table, TContextable, TConfig } from "./table";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

type ActionProto = { name?: string; fileName?: string; dependencies?: string[] };

export class Session {
  public rootDir: string;

  public config: protos.IProjectConfig;

  public tables: { [name: string]: Table };
  public operations: { [name: string]: Operation };
  public assertions: { [name: string]: Assertion };

  public graphErrors: protos.IGraphErrors;

  constructor(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.init(rootDir, projectConfig);
  }

  init(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.rootDir = rootDir;
    this.config = projectConfig || { defaultSchema: "dataform" };
    this.tables = {};
    this.operations = {};
    this.assertions = {};
    this.graphErrors = { compilationErrors: [] };
  }

  adapter(): adapters.IAdapter {
    return adapters.create(this.config);
  }

  target(name: string): protos.ITarget {
    if (name.includes(".")) {
      var schema = name.split(".")[0];
      var name = name.split(".")[1];
      return protos.Target.create({ name, schema });
    } else {
      return protos.Target.create({
        name,
        schema: this.config.defaultSchema
      });
    }
  }

  ref(name: string): string {
    const tNode = this.tables[name];
    const oNode = this.operations[name];

    if (tNode) {
      return this.adapter().resolveTarget((tNode as Table).proto.target);
    } else if (oNode && oNode.proto.hasOutput) {
      return this.adapter().resolveTarget((oNode as Operation).proto.target);
    } else {
      const message = `Could not find referenced node: ${name}`;
      this.compileError(new Error(message));
    }
  }

  operate(name: string, queries?: OContextable<string | string[]>): Operation {
    var operation = new Operation();
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

  materialize(name: string, queryOrConfig?: TContextable<string> | TConfig): Table {
    return this.publish(name, queryOrConfig);
  }

  publish(name: string, queryOrConfig?: TContextable<string> | TConfig): Table {
    // Check for duplicate names
    if (this.tables[name]) {
      const message = `Duplicate node name detected, names must be unique across tables, assertions, and operations: "${name}"`;
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

  assert(name: string, query?: AContextable<string>): Assertion {
    var assertion = new Assertion();
    assertion.session = this;
    assertion.proto.name = name;
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.assertions[name] = assertion;
    return assertion;
  }

  compileError(err: Error, path?: string) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;

    const compileError = protos.CompilationError.create({ stack: err.stack, fileName, message: err.message });
    this.graphErrors.compilationErrors.push(compileError);
  }

  compileGraphChunk<T>(part: { [name: string]: { proto: ActionProto; compile(): T } }): Array<T> {
    const compiledChunks: Array<T> = [];

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

  compile(): protos.ICompiledGraph {
    const compiledGraph = protos.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.tables),
      operations: this.compileGraphChunk(this.operations),
      assertions: this.compileGraphChunk(this.assertions),
      graphErrors: this.graphErrors
    });

    return compiledGraph;
  }
}
