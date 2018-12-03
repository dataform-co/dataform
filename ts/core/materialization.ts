import { Dataform } from "./index";
import * as protos from "@dataform/protos";

const materializationType = {
  table: "",
  view: "",
  incremental: ""
};

export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type MaterializationType = keyof typeof materializationType;

export interface MConfig {
  type?: MaterializationType;
  query?: MContextable<string>;
  where?: MContextable<string>;
  preOps?: MContextable<string | string[]>;
  postOps?: MContextable<string | string[]>;
  dependencies?: string | string[];
  descriptor?: { [key: string]: string };
  disabled?: boolean;
}

export class Materialization {
  proto: protos.Materialization = protos.Materialization.create({
    type: "view",
    disabled: false
  });

  // Hold a reference to the Dataform instance.
  dataform: Dataform;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: MContextable<string>;
  private contextableWhere: MContextable<string>;
  private contextablePreOps: MContextable<string | string[]>[] = [];
  private contextablePostOps: MContextable<string | string[]>[] = [];

  public config(config: MConfig) {
    if (config.type) {
      this.type(config.type);
    }
    if (config.query) {
      this.query(config.query);
    }
    if (config.where) {
      this.where(config.where);
    }
    if (config.preOps) {
      this.preOps(config.preOps);
    }
    if (config.postOps) {
      this.postOps(config.postOps);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.descriptor) {
      this.descriptor(config.descriptor);
    }
    if (config.disabled) {
      this.disabled();
    }
    return this;
  }

  public type(type: MaterializationType) {
    if (materializationType.hasOwnProperty(type)) {
      this.proto.type = type;
    } else {
      const predefinedTypes = Object.keys(materializationType)
        .map(item => `"${item}"`)
        .join(" | ");
      throw Error(`Wrong type of materialization detected. Should only use predefined types: ${predefinedTypes}`);
    }

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

  public preOps(pres: MContextable<string | string[]>) {
    this.contextablePreOps.push(pres);
    return this;
  }

  public postOps(posts: MContextable<string | string[]>) {
    this.contextablePostOps.push(posts);
    return this;
  }

  public disabled() {
    this.proto.disabled = true;
    return this;
  }

  public dependencies(value: string | string[]) {
    var newDependencies = typeof value === "string" ? [value] : value;
    newDependencies.forEach(d => {
      if (this.proto.dependencies.indexOf(d) < 0) {
        this.proto.dependencies.push(d);
      }
    });
    return this;
  }

  public descriptor(key: string, description?: string);
  public descriptor(map: { [key: string]: string });
  public descriptor(keys: string[]);
  public descriptor(keyOrKeysOrMap: string | string[] | { [key: string]: string }, description?: string) {
    if (!this.proto.descriptor) {
      this.proto.descriptor = {};
    }
    if (typeof keyOrKeysOrMap === "string") {
      this.proto.descriptor[keyOrKeysOrMap] = description || "";
    } else if (keyOrKeysOrMap instanceof Array) {
      keyOrKeysOrMap.forEach(key => {
        this.proto.descriptor[key] = "";
      });
    } else {
      Object.keys(keyOrKeysOrMap).forEach(key => {
        this.proto.descriptor[key] = keyOrKeysOrMap[key] || "";
      });
    }
    return this;
  }

  compile() {
    var context = new MaterializationContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    this.contextableQuery = null;

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
      this.contextableWhere = null;
    }

    this.contextablePreOps.forEach(contextablePreOps => {
      var appliedPres = context.apply(contextablePreOps);
      this.proto.preOps = (this.proto.preOps || []).concat(
        typeof appliedPres == "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePreOps = [];

    this.contextablePostOps.forEach(contextablePostOps => {
      var appliedPosts = context.apply(contextablePostOps);
      this.proto.postOps = (this.proto.postOps || []).concat(
        typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePostOps = [];

    return this.proto;
  }
}

export class MaterializationContext {
  private materialization?: Materialization;

  constructor(materialization: Materialization) {
    this.materialization = materialization;
  }

  public config(config: MConfig) {
    this.materialization.config(config);
    return "";
  }
  public self(): string {
    return this.materialization.dataform.adapter().resolveTarget(this.materialization.proto.target);
  }

  public ref(name: string) {
    this.materialization.dependencies(name);
    return this.materialization.dataform.ref(name);
  }

  public type(type: MaterializationType) {
    this.materialization.type(type);
    return "";
  }

  public where(where: MContextable<string>) {
    this.materialization.where(where);
    return "";
  }

  public preOps(statement: MContextable<string | string[]>) {
    this.materialization.preOps(statement);
    return "";
  }

  public postOps(statement: MContextable<string | string[]>) {
    this.materialization.postOps(statement);
    return "";
  }

  public disabled() {
    this.materialization.disabled();
    return "";
  }

  public dependencies(name: string) {
    this.materialization.dependencies(name);
    return "";
  }

  public descriptor(key: string, description?: string);
  public descriptor(map: { [key: string]: string });
  public descriptor(keys: string[]);
  public descriptor(keyOrKeysOrMap: string | string[] | { [key: string]: string }, description?: string) {
    this.materialization.descriptor(keyOrKeysOrMap as any, description);
    return "";
  }

  public describe(key: string, description?: string) {
    this.materialization.descriptor(key, description);
    return key;
  }

  public apply<T>(value: MContextable<T>): T {
    if (typeof value === "function") {
      return value(this);
    } else {
      return value;
    }
  }
}
