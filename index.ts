import * as protos from "./protos";
import * as adapters from "./adapters";

if (require.extensions) {
  require.extensions[".sql"] = function(module: any, file: string) {
    var pathSplits = file.split("/");
    var fileBasename = pathSplits[pathSplits.length - 1].split(".")[0];
    var oldCompile = module._compile;
    module._compile = function(code, file) {
      module._compile = oldCompile;
      var newCode = `
      materialize("${fileBasename}").query(ctx => {
        const type = ctx.type.bind(ctx);
        const post = ctx.post.bind(ctx);
        const pre = ctx.pre.bind(ctx);
        const ref = ctx.ref.bind(ctx);
        const dependency = ctx.dependency.bind(ctx);
        return \`${code}\`;
      })`;
      module._compile(newCode, file);
    };
    require.extensions[".js"](module, file);
  };
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
    return new adapters.GenericAdapter(this.projectConfig);
  }

  target(name: string, schema?: string) {
    return protos.Target.create({ name: name, schema: schema || this.projectConfig.defaultSchema });
  }

  operation(name: string, statements: OContextable<string | string[]>): Operation {
    var operation = new Operation();
    operation.dft = this;
    operation.proto.name = name;
    operation.contextableStatements = statements;
    this.nodes[name] = operation;
    return operation;
  }

  materialize(name: string) {
    var materialization = new Materialization();
    materialization.dft = this;
    materialization.proto.name = name;
    materialization.proto.target = this.target(name);
    // Add it to global index.
    this.nodes[name] = materialization;
    return materialization;
  }

  compile() {
    return Object.keys(this.nodes).map(key => {
      this.nodes[key].compile();
      return this.nodes[key].proto;
    });
  }

  build(runConfig: protos.IRunConfig) {
    this.compile();
    return Object.keys(this.nodes).map(key => {
      this.nodes[key].build(runConfig);
      return this.nodes[key].proto;
    });
  }
}

const singleton = new Dft();

export const materialize = (name: string) => singleton.materialize(name);
export const operation = (name: string, statements: OContextable<string | string[]>) =>
  singleton.operation(name, statements);
export const compile = () => singleton.compile();
export const build = (runConfig: protos.IRunConfig) => singleton.build(runConfig);
export const init = (projectConfig?: protos.IProjectConfig) => singleton.init(projectConfig);

(global as any).materialize = materialize;
(global as any).operation = operation;

export interface Node {
  proto: {
    name?: string;
  };
  compile();
  build(runConfig: protos.IRunConfig);
}

export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type OContextable<T> = T | ((ctx: OperationContext) => T);

export class Materialization implements Node {
  proto: protos.Materialization = protos.Materialization.create({ type: protos.MaterializationType.VIEW });

  // Hold a reference to the Dft instance.
  dft: Dft;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: MContextable<string>;
  private contextablePres: MContextable<string | string[]>;
  private contextablePosts: MContextable<string | string[]>;

  public type(type: string) {
    this.proto.type = protos.MaterializationType[type.toUpperCase()];
    return this;
  }

  public query(query: MContextable<string>) {
    this.contextableQuery = query;
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

  public dependency(value: string) {
    this.proto.dependencies.push(value);
    return this;
  }

  compile() {
    var context = new MaterializationContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    this.contextableQuery = null;

    if (this.contextablePres) {
      var appliedPres = context.apply(this.contextablePres);
      this.proto.pres = typeof appliedPres == "string" ? [appliedPres] : appliedPres;
      this.contextablePres = null;
    }

    if (this.contextablePosts) {
      var appliedPosts = context.apply(this.contextablePosts);
      this.proto.posts = typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts;
      this.contextablePosts = null;
    }
  }

  build(runConfig: protos.IRunConfig) {
    this.proto.compiledStatements = [];
    this.proto.compiledStatements = this.proto.compiledStatements.concat(this.proto.pres);
    this.proto.compiledStatements = this.proto.compiledStatements.concat(
      this.dft.adapter().materializeStatements(this.proto, runConfig)
    );
    this.proto.compiledStatements = this.proto.compiledStatements.concat(this.proto.posts);
  }
}

export class MaterializationContext {
  private materialization?: Materialization;

  constructor(materialization: Materialization) {
    this.materialization = materialization;
  }

  public type(type: string) {
    this.materialization.type(type);
    return "";
  }

  public this(): string {
    return this.materialization.dft.adapter().queryableName(this.materialization.proto.target);
  }

  public ref(name: string) {
    var refNode = this.materialization.dft.nodes[name];
    if (refNode && refNode instanceof Materialization) {
      this.materialization.proto.dependencies.push(name);
      return this.materialization.dft.adapter().queryableName((refNode as Materialization).proto.target);
    } else {
      var err = `Could not find reference node (${name}) in nodes [${Object.keys(this.materialization.dft.nodes)}]`;
      throw err;
    }
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
  }

  public apply<T>(value: MContextable<T>): T {
    if (typeof value === "string") {
      return value;
    } else {
      return (value as any)(this);
    }
  }
}

export class Operation implements Node {
  proto: protos.IOperation = protos.Operation.create();

  // Hold a reference to the Dft instance.
  dft: Dft;

  // We delay contextification until the final compile step, so hold these here for now.
  contextableStatements: OContextable<string | string[]>;

  public dependency(value: string) {
    this.proto.dependencies.push(value);
    return this;
  }

  compile() {
    var context = new OperationContext(this);

    var appliedStatements = context.apply(this.contextableStatements);
    this.proto.statements = typeof appliedStatements == "string" ? [appliedStatements] : appliedStatements;
    this.contextableStatements = null;
  }

  build(runConfig: protos.IRunConfig) {}
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
      return this.operation.dft.adapter().queryableName((refNode as Materialization).proto.target);
    } else {
      throw "Could not find reference node";
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

  compile() {}
  build(runConfig: protos.IRunConfig) {}
}
