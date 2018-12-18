import { Session } from "./index";
import * as protos from "@dataform/protos";

export enum MaterializationTypes {
  TABLE = "table",
  VIEW = "view",
  INCREMENTAL = "incremental"
}
export enum DistStyleTypes {
  EVEN = "even",
  KEY = "key",
  ALL = "all"
}
export enum SortStyleTypes {
  COMPOUND = "compound",
  INTERLEAVED = "interleaved"
}

type ValueOf<T> = T[keyof T];
export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type MaterializationType = ValueOf<MaterializationTypes>;

export interface MConfig {
  type?: MaterializationType;
  query?: MContextable<string>;
  where?: MContextable<string>;
  preOps?: MContextable<string | string[]>;
  postOps?: MContextable<string | string[]>;
  dependencies?: string | string[];
  descriptor?: { [key: string]: string };
  disabled?: boolean;
  redshift?: protos.IRedshift;
}

export class Materialization {
  proto: protos.Materialization = protos.Materialization.create({
    type: "view",
    disabled: false,
    validationErrors: []
  });

  // Hold a reference to the Session instance.
  session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: MContextable<string>;
  private contextableWhere: MContextable<string>;
  private contextablePreOps: MContextable<string | string[]>[] = [];
  private contextablePostOps: MContextable<string | string[]>[] = [];

  private getPredefinedTypes(types) {
    return Object.keys(types)
      .map(key => `"${types[key]}"`)
      .join(" | ");
  }

  private isValidProps(props: { [x: string]: string | string[] }, types: { [x: string]: any }) {
    const propsValid = Object.keys(props).every(key => {
      if (!props[key] || !props[key].length) {
        const message = `Property "${key}" is not defined`;
        this.validationError(message);
        return false;
      }
      return true;
    });

    const typesValid = Object.keys(types).every(type => {
      const currentEnum = types[type];
      if (
        Object.keys(currentEnum)
          .map(key => currentEnum[key])
          .indexOf(props[type]) === -1
      ) {
        const predefinedValues = this.getPredefinedTypes(currentEnum);
        const message = `Wrong value of "${type}" property. Should only use predefined values: ${predefinedValues}`;
        this.validationError(message);
        return false;
      }
      return true;
    });

    return propsValid && typesValid;
  }

  public config(config: MConfig) {
    if (config.where) {
      this.where(config.where);
    }
    if (config.type) {
      this.type(config.type);
    }
    if (config.query) {
      this.query(config.query);
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
    if (config.redshift) {
      this.redshift(config.redshift);
    }
    return this;
  }

  public validationError(message: string) {
    let fileName = this.proto.fileName || __filename;

    var validationError = protos.ValidationError.create({ fileName, message });
    this.proto.validationErrors.push(validationError);
  }

  public type(type: MaterializationType) {
    if (
      Object.keys(MaterializationTypes)
        .map(key => MaterializationTypes[key])
        .indexOf(type) === -1
    ) {
      const predefinedTypes = this.getPredefinedTypes(MaterializationTypes);
      const message = `Wrong type of materialization detected. Should only use predefined types: ${predefinedTypes}`;
      this.validationError(message);
      return this;
    } else if (type === MaterializationTypes.INCREMENTAL && !this.contextableWhere) {
      const message = `"where" property is not defined. With the type “incremental” you must first specify the property “where”!`;
      this.validationError(message);
      return this;
    }

    this.proto.type = type as string;
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

  public redshift(redshift: protos.IRedshift) {
    if (Object.keys(redshift).length === 0) {
      const message = `Missing properties in redshift config`;
      this.validationError(message);
      return this;
    }
    if (redshift.distStyle || redshift.distKey) {
      const props = { distStyle: redshift.distStyle, distKey: redshift.distKey };
      if (!this.isValidProps(props, { distStyle: DistStyleTypes })) {
        return this;
      }
    }
    if (redshift.sortStyle || redshift.sortKeys) {
      const props = { sortStyle: redshift.sortStyle, sortKeys: redshift.sortKeys };
      if (!this.isValidProps(props, { sortStyle: SortStyleTypes })) {
        return this;
      }
    }

    this.proto.redshift = protos.Redshift.create(redshift);
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
    return this.materialization.session.adapter().resolveTarget(this.materialization.proto.target);
  }

  public ref(name: string) {
    this.materialization.dependencies(name);
    return this.materialization.session.ref(name);
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

  public redshift(redshift: protos.IRedshift) {
    this.materialization.redshift(redshift);
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
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
