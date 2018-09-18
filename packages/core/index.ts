import * as protos from "./protos";
import * as adapters from "./adapters";
import * as utils from "./utils";
import * as parser from "./parser";

export { protos, adapters, utils, parser };

export type WarehouseType = "bigquery" | "redshift" | "postgres" | "snowflake";
export type MaterializationType = "table" | "view" | "incremental";

if (require.extensions) {
  require.extensions[".sql"] = function(module: any, file: string) {
    var oldCompile = module._compile;
    module._compile = function(code, file) {
      module._compile = oldCompile;
      module._compile(utils.compileSql(code, file), file);
    };
    require.extensions[".js"](module, file);
  };
}

function patternToRegex(pattern: string) {
  return new RegExp(
    "^" +
      pattern
        .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
        .split("*")
        .join(".*") +
      "$"
  );
}

function matchPatterns(patterns: string[], values: string[]) {
  var regexps = patterns.map(pattern => patternToRegex(pattern));
  return values.filter(
    value => regexps.filter(regexp => regexp.test(value)).length > 0
  );
}

export class Dft {
  projectConfig: protos.IProjectConfig;
  nodes: { [name: string]: Node };

  constructor(projectConfig?: protos.IProjectConfig) {
    this.init(projectConfig);
  }

  init(projectConfig?: protos.IProjectConfig) {
    this.projectConfig = projectConfig || { defaultSchema: "dataform" };
    this.nodes = {};
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

  operate(
    name: string,
    statement?: OContextable<string | string[]>
  ): Operation {
    var operation = new Operation();
    operation.dft = this;
    operation.proto.name = name;
    if (statement) {
      operation.statement(statement);
    }
    // Add it to global index.
    this.nodes[name] = operation;
    return operation;
  }

  materialize(name: string, query?: MContextable<string>): Materialization {
    var materialization = new Materialization();
    materialization.dft = this;
    materialization.proto.name = name;
    materialization.proto.target = this.target(name);
    if (query) {
      materialization.query(query);
    }
    // Add it to global index.
    this.nodes[name] = materialization;
    return materialization;
  }

  assert(name: string, query?: AContextable<string | string[]>): Assertion {
    var assertion = new Assertion();
    assertion.dft = this;
    assertion.proto.name = name;
    if (query) {
      assertion.query(query);
    }
    // Add it to global index.
    this.nodes[name] = assertion;
    return assertion;
  }

  compile() {
    return Object.keys(this.nodes).map(key => {
      this.nodes[key].compile();
      return this.nodes[key].proto;
    });
  }

  build(runConfig: protos.IRunConfig): protos.IExecutionGraph {
    this.compile();
    var includedNodeNames =
      runConfig.nodes && runConfig.nodes.length > 0
        ? matchPatterns(runConfig.nodes, Object.keys(this.nodes))
        : Object.keys(this.nodes);
    if (runConfig.includeDependencies) {
      // Compute all transitive dependencies.
      for (let i = 0; i < Object.keys(this.nodes).length; i++) {
        includedNodeNames.forEach(includedName => {
          var node = this.nodes[includedName].proto;
          var matchingNodeNames =
            node.dependencies && node.dependencies.length > 0
              ? matchPatterns(node.dependencies, Object.keys(this.nodes))
              : [];
          matchingNodeNames.forEach(nodeName => {
            if (includedNodeNames.indexOf(nodeName) < 0) {
              includedNodeNames.push(nodeName);
            }
          });
        });
      }
    }
    return {
      projectConfig: this.projectConfig,
      runConfig: runConfig,
      nodes: includedNodeNames.map(key => {
        var node = this.nodes[key].build(runConfig);
        // Remove any excluded dependencies and evaluate wildcard dependencies.
        node.dependencies = matchPatterns(node.dependencies, includedNodeNames);
        return node;
      })
    };
  }
}

const singleton = new Dft();

export const materialize = (name: string, query?: MContextable<string>) =>
  singleton.materialize(name, query);
export const operate = (
  name: string,
  statement?: OContextable<string | string[]>
) => singleton.operate(name, statement);
export const assert = (name: string, query?: AContextable<string | string[]>) =>
  singleton.assert(name, query);
export const compile = () => singleton.compile();
export const build = (runConfig: protos.IRunConfig) =>
  singleton.build(runConfig);
export const init = (projectConfig?: protos.IProjectConfig) =>
  singleton.init(projectConfig);

(global as any).materialize = materialize;
(global as any).operate = operate;
(global as any).assert = assert;

export interface Node {
  proto: {
    name?: string;
    dependencies?: string[];
  };
  compile();
  build(runConfig: protos.IRunConfig): protos.IExecutionNode;
}

export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type OContextable<T> = T | ((ctx: OperationContext) => T);
export type AContextable<T> = T | ((ctx: AssertionContext) => T);

export class Materialization implements Node {
  proto: protos.Materialization = protos.Materialization.create({
    type: "view"
  });

  // Hold a reference to the Dft instance.
  dft: Dft;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: MContextable<string>;
  private contextableWhere: MContextable<string>;
  private contextablePres: MContextable<string | string[]>;
  private contextablePosts: MContextable<string | string[]>;
  private contextableAssertions: MContextable<string | string[]>;

  public type(type: MaterializationType) {
    this.proto.type = type;
    return this;
  }

  public query(query: MContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  public where(where: MContextable<string>) {
    this.contextableWhere = where;
    return this;
  }

  public pre(pres: MContextable<string | string[]>) {
    this.contextablePres = pres;
    return this;
  }

  public post(posts: MContextable<string | string[]>) {
    this.contextablePosts = posts;
    return this;
  }

  public assert(query: MContextable<string | string[]>) {
    this.contextableAssertions = query;
  }

  public dependency(value: string) {
    this.proto.dependencies.push(value);
    return this;
  }

  public describe(key: string, description: string);
  public describe(map: { [key: string]: string });
  public describe(
    keyOrMap: string | { [key: string]: string },
    description?: string
  ) {
    if (!!this.proto.descriptions) {
      this.proto.descriptions = {};
    }
    if (typeof keyOrMap === "string") {
      this.proto.descriptions[keyOrMap] = description;
    } else {
      Object.assign(this.proto.descriptions, keyOrMap);
    }
  }

  compile() {
    var context = new MaterializationContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    this.contextableQuery = null;

    this.proto.where = context.apply(this.contextableWhere);
    this.contextableWhere = null;

    if (this.contextablePres) {
      var appliedPres = context.apply(this.contextablePres);
      this.proto.pres =
        typeof appliedPres == "string" ? [appliedPres] : appliedPres;
      this.contextablePres = null;
    }

    if (this.contextablePosts) {
      var appliedPosts = context.apply(this.contextablePosts);
      this.proto.posts =
        typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts;
      this.contextablePosts = null;
    }

    if (this.contextableAssertions) {
      var appliedAssertions = context.apply(this.contextableAssertions);
      this.proto.assertions =
        typeof appliedAssertions == "string"
          ? [appliedAssertions]
          : appliedAssertions;
      this.contextableAssertions = null;
    }

    // Compute columns.
    try {
      var tree = parser.parse(this.proto.query, {});
      var parsedColumns = tree.statement[0].result.map(res => res.alias);
      if (parsedColumns.indexOf(null) < 0) {
        this.proto.parsedColumns = parsedColumns;
      }
    } catch (e) {
      // There was an exception parsing the columns, ignore.
    }
  }

  build(runConfig: protos.IRunConfig) {
    return protos.ExecutionNode.create({
      name: this.proto.name,
      dependencies: this.proto.dependencies,
      tasks: ([] as protos.IExecutionTask[]).concat(
        this.proto.pres.map(pre => ({ statement: pre })),
        this.dft.adapter().build(this.proto, runConfig),
        this.proto.posts.map(post => ({ statement: post })),
        this.proto.assertions.map(assertion => ({
          statement: assertion,
          type: "assertion"
        }))
      )
    });
  }
}

export class MaterializationContext {
  private materialization?: Materialization;

  constructor(materialization: Materialization) {
    this.materialization = materialization;
  }

  public self(): string {
    return this.materialization.dft
      .adapter()
      .queryableName(this.materialization.proto.target);
  }

  public ref(name: string) {
    var refNode = this.materialization.dft.nodes[name];
    if (refNode && refNode instanceof Materialization) {
      this.materialization.proto.dependencies.push(name);
      return this.materialization.dft
        .adapter()
        .queryableName((refNode as Materialization).proto.target);
    } else {
      throw `Could not find reference node (${name}) in nodes [${Object.keys(
        this.materialization.dft.nodes
      )}]`;
    }
  }

  public type(type: MaterializationType) {
    this.materialization.type(type);
    return "";
  }

  public where(where: MContextable<string>) {
    this.materialization.where(where);
    return "";
  }

  public pre(statement: MContextable<string | string[]>) {
    this.materialization.pre(statement);
    return "";
  }

  public post(statement: MContextable<string | string[]>) {
    this.materialization.post(statement);
    return "";
  }

  public dependency(name: string) {
    this.materialization.proto.dependencies.push(name);
    return "";
  }

  public assert(query: MContextable<string | string[]>) {
    this.materialization.assert(query);
    return "";
  }

  public describe(key: string, description: string);
  public describe(map: { [key: string]: string });
  public describe(
    keyOrMap: string | { [key: string]: string },
    description?: string
  ) {
    this.materialization.describe(keyOrMap as any, description);
    if (typeof keyOrMap == "string") {
      return keyOrMap;
    }
    return "";
  }

  public apply<T>(value: MContextable<T>): T {
    if (typeof value === "function") {
      return value(this);
    } else {
      return value;
    }
  }
}

export class Operation implements Node {
  proto: protos.IOperation = protos.Operation.create();

  // Hold a reference to the Dft instance.
  dft: Dft;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableStatements: OContextable<string | string[]>;

  public statement(statement: OContextable<string | string[]>) {
    this.contextableStatements = statement;
  }

  public dependency(value: string) {
    this.proto.dependencies.push(value);
    return this;
  }

  compile() {
    var context = new OperationContext(this);

    var appliedStatements = context.apply(this.contextableStatements);
    this.proto.statements =
      typeof appliedStatements == "string"
        ? [appliedStatements]
        : appliedStatements;
    this.contextableStatements = null;
  }

  build(runConfig: protos.IRunConfig) {
    return protos.ExecutionNode.create({
      dependencies: this.proto.dependencies,
      name: this.proto.name,
      tasks: this.proto.statements.map(statement => ({
        type: "statement",
        statement: statement
      }))
    });
  }
}

export class OperationContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public ref(name: string) {
    var refNode = this.operation.dft.nodes[name];
    if (refNode && refNode instanceof Materialization) {
      this.operation.proto.dependencies.push(name);
      return this.operation.dft
        .adapter()
        .queryableName((refNode as Materialization).proto.target);
    } else {
      throw `Could not find reference node (${name}) in nodes [${Object.keys(
        this.operation.dft.nodes
      )}]`;
    }
  }

  public dependency(name: string) {
    this.operation.proto.dependencies.push(name);
  }

  public apply<T>(value: OContextable<T>): T {
    if (typeof value === "string") {
      return value;
    } else {
      return (value as any)(this);
    }
  }
}

export class Assertion implements Node {
  proto: protos.IAssertion = protos.Assertion.create();

  // Hold a reference to the Dft instance.
  dft: Dft;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: AContextable<string | string[]>;

  public query(query: AContextable<string | string[]>) {
    this.contextableQueries = query;
  }

  compile() {
    var context = new AssertionContext(this);

    var appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries =
      typeof appliedQueries == "string" ? [appliedQueries] : appliedQueries;
    this.contextableQueries = null;
  }

  build(runConfig: protos.IRunConfig) {
    return protos.ExecutionNode.create({
      name: this.proto.name,
      dependencies: this.proto.dependencies,
      tasks: this.proto.queries.map(query => ({
        type: "assertion",
        statement: query
      }))
    });
  }
}

export class AssertionContext {
  private assertion?: Assertion;

  constructor(assertion: Assertion) {
    this.assertion = assertion;
  }

  public ref(name: string) {
    var refNode = this.assertion.dft.nodes[name];
    if (refNode && refNode instanceof Materialization) {
      this.assertion.proto.dependencies.push(name);
      return this.assertion.dft
        .adapter()
        .queryableName((refNode as Materialization).proto.target);
    } else {
      throw `Could not find reference node (${name}) in nodes [${Object.keys(
        this.assertion.dft.nodes
      )}]`;
    }
  }

  public apply<T>(value: AContextable<T>): T {
    if (typeof value === "string") {
      return value;
    } else {
      return (value as any)(this);
    }
  }
}
