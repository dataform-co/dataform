import * as protos from "./protos";
import * as adapters from "./adapters";
import * as utils from "./utils";
import * as parser from "./parser";
import { Materialization, MContextable } from "./materialization";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

export class Dataform {
  projectConfig: protos.IProjectConfig;

  materializations: { [name: string]: Materialization };
  operations: { [name: string]: Operation };
  assertions: { [name: string]: Assertion };

  constructor(projectConfig?: protos.IProjectConfig) {
    this.init(projectConfig);
  }

  init(projectConfig?: protos.IProjectConfig) {
    this.projectConfig = projectConfig || { defaultSchema: "dataform" };
    this.materializations = {};
    this.operations = {};
    this.assertions = {};
  }

  adapter(): adapters.Adapter {
    return adapters.create(this.projectConfig);
  }

  target(name: string, schema?: string) {
    return protos.Target.create({
      name: name,
      schema: schema || this.projectConfig.defaultSchema
    });
  }

  ref(name: string): string {
    var refNode = this.materializations[name];
    if (refNode) {
      return this.adapter().queryableName(
        (refNode as Materialization).proto.target
      );
    } else {
      throw `Could not find reference node (${name}) in nodes [${Object.keys(
        this.materializations
      )}]`;
    }
  }

  operate(
    name: string,
    statement?: OContextable<string | string[]>
  ): Operation {
    var operation = new Operation();
    operation.dataform = this;
    operation.proto.name = name;
    if (statement) {
      operation.statement(statement);
    }
    // Add it to global index.
    this.operations[name] = operation;
    return operation;
  }

  materialize(name: string, query?: MContextable<string>): Materialization {
    var materialization = new Materialization();
    materialization.dataform = this;
    materialization.proto.name = name;
    materialization.proto.target = this.target(name);
    if (query) {
      materialization.query(query);
    }
    // Add it to global index.
    this.materializations[name] = materialization;
    return materialization;
  }

  assert(name: string, query?: AContextable<string | string[]>): Assertion {
    var assertion = new Assertion();
    assertion.dataform = this;
    assertion.proto.name = name;
    if (query) {
      assertion.query(query);
    }
    // Add it to global index.
    this.assertions[name] = assertion;
    return assertion;
  }

  compile() {
    return protos.CompiledGraph.create({
      projectConfig: this.projectConfig,
      materializations: Object.keys(this.materializations).map(key =>
        this.materializations[key].compile()
      ),
      operations: Object.keys(this.operations).map(key =>
        this.operations[key].compile()
      ),
      assertions: Object.keys(this.assertions).map(key =>
        this.assertions[key].compile()
      )
    });
  }
}
