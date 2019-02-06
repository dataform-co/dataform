import * as protos from "@dataform/protos";
import * as adapters from "./adapters";
import * as utils from "./utils";
import { Table, TContextable, TConfig } from "./table";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

export class Session {
  public rootDir: string;

  public config: protos.IProjectConfig;

  public tables: { [name: string]: Table };
  public operations: { [name: string]: Operation };
  public assertions: { [name: string]: Assertion };

  public validationErrors: protos.IValidationError[];
  public compileErrors: protos.ICompileError[];

  constructor(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.init(rootDir, projectConfig);
  }

  init(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.rootDir = rootDir;
    this.config = projectConfig || { defaultSchema: "dataform" };
    this.tables = {};
    this.operations = {};
    this.assertions = {};
    this.validationErrors = [];
    this.compileErrors = [];
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
      this.validationError(message);
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
      this.validationError(message);
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

  validationError(message: string) {
    const fileName = utils.getCallerFile(this.rootDir) || __filename;

    const validationError = protos.ValidationError.create({ fileName, message });
    this.validationErrors.push(validationError);
  }

  compileError(err: Error, path?: string) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;
    const stData = utils.getStackTraceData(err);

    const compileError = protos.CompileError.create({ ...stData, fileName, message: err.message });
    this.compileErrors.push(compileError);
  }

  compileGraphChunk(part: { [name: string]: Table | Operation | Assertion }): Array<any> {
    const compiledChunks = [];

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
    var compiledGraph = protos.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.tables),
      operations: this.compileGraphChunk(this.operations),
      assertions: this.compileGraphChunk(this.assertions),
      validationErrors: this.validationErrors,
      compileErrors: this.compileErrors
    });

    // Check there aren't any duplicate names.
    var allNodes = [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations);
    var allNodeNames = allNodes.map(node => node.name);

    // Check there are no duplicate node names.
    allNodes.forEach(node => {
      if (allNodes.filter(subNode => subNode.name == node.name).length > 1) {
        const message = `Duplicate node name detected, names must be unique across tables, assertions, and operations: "${
          node.name
        }"`;
        this.validationError(message);
      }
    });

    // Expand node dependency wilcards.
    allNodes.forEach(node => {
      var uniqueDeps: { [d: string]: boolean } = {};
      // Add non-wildcard deps normally.
      node.dependencies.filter(d => !d.includes("*")).forEach(d => (uniqueDeps[d] = true));
      // Match wildcard deps against all node names.
      utils
        .matchPatterns(node.dependencies.filter(d => d.includes("*")), allNodeNames)
        .forEach(d => (uniqueDeps[d] = true));
      node.dependencies = Object.keys(uniqueDeps);
    });

    var nodesByName: { [name: string]: protos.IExecutionNode } = {};
    allNodes.forEach(node => (nodesByName[node.name] = node));

    // Check all dependencies actually exist.
    allNodes.forEach(node => {
      node.dependencies.forEach(dependency => {
        if (allNodeNames.indexOf(dependency) < 0) {
          const message = `Missing dependency detected: Node "${
            node.name
          }" depends on "${dependency}" which does not exist.`;
          this.validationError(message);
        }
      });
    });

    // Check for circular dependencies.
    const checkCircular = (node: protos.IExecutionNode, dependents: protos.IExecutionNode[]): boolean => {
      if (dependents.indexOf(node) >= 0) {
        const message = `Circular dependency detected in chain: [${dependents.map(d => d.name).join(" > ")} > ${
          node.name
        }]`;
        this.validationError(message);
        return true;
      }
      return node.dependencies.some(d => {
        return nodesByName[d] && checkCircular(nodesByName[d], dependents.concat([node]));
      });
    };

    for (let i = 0; i < allNodes.length; i++) {
      if (checkCircular(allNodes[i], [])) {
        break;
      }
    }

    return compiledGraph;
  }
}
