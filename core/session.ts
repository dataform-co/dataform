import * as protos from "@dataform/protos";
import * as adapters from "./adapters";
import * as utils from "./utils";
import { Materialization, MContextable, MConfig } from "./materialization";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

export class Session {

  public rootDir: string;

  public config: protos.IProjectConfig;

  public materializations: { [name: string]: Materialization };
  public operations: { [name: string]: Operation };
  public assertions: { [name: string]: Assertion };

  constructor(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.init(rootDir, projectConfig);
  }

  init(rootDir: string, projectConfig?: protos.IProjectConfig) {
    this.rootDir = rootDir;
    this.config = projectConfig || { defaultSchema: "dataform" };
    this.materializations = {};
    this.operations = {};
    this.assertions = {};
  }

  adapter(): adapters.Adapter {
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
    var refNode = this.materializations[name];
    if (refNode) {
      return this.adapter().resolveTarget((refNode as Materialization).proto.target);
    } else {
      throw `Could not find reference node (${name}) in nodes [${Object.keys(this.materializations)}]`;
    }
  }

  operate(name: string, queries?: OContextable<string | string[]>): Operation {
    var operation = new Operation();
    operation.session = this;
    operation.proto.name = name;
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.operations[name] = operation;
    return operation;
  }

  materialize(name: string, queryOrConfig?: MContextable<string> | MConfig): Materialization {
    var materialization = new Materialization();
    materialization.session = this;
    materialization.proto.name = name;
    materialization.proto.target = this.target(name);
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        materialization.config(queryOrConfig);
      } else {
        materialization.query(queryOrConfig);
      }
    }
    materialization.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.materializations[name] = materialization;
    return materialization;
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

  compile(): protos.ICompiledGraph {
    var compiledGraph = protos.CompiledGraph.create({
      projectConfig: this.config,
      materializations: Object.keys(this.materializations).map(key => this.materializations[key].compile()),
      operations: Object.keys(this.operations).map(key => this.operations[key].compile()),
      assertions: Object.keys(this.assertions).map(key => this.assertions[key].compile())
    });

    // Check there aren't any duplicate names.
    var allNodes = [].concat(compiledGraph.materializations, compiledGraph.assertions, compiledGraph.operations);
    var allNodeNames = allNodes.map(node => node.name);

    // Check there are no duplicate node names.
    allNodes.forEach(node => {
      if (allNodes.filter(subNode => subNode.name == node.name).length > 1) {
        throw Error(
          `Duplicate node name detected, names must be unique across materializations, assertions, and operations: "${
            node.name
          }"`
        );
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
          throw Error(`Missing dependency detected: Node "${node.name}" depends on "${dependency}" which does not exist.`);
        }
      });
    });
    // Check for circular dependencies.
    function checkCircular(node: protos.IExecutionNode, dependents: protos.IExecutionNode[]) {
      if (dependents.indexOf(node) >= 0) {
        throw Error(
          `Circular dependency detected in chain: [${dependents.map(d => d.name).join(" > ")} > ${node.name}]`
        );
      }
      node.dependencies.forEach(d => checkCircular(nodesByName[d], dependents.concat([node])));
    }
    allNodes.forEach(node => checkCircular(node, []));
    return compiledGraph;
  }
}
