import { Dataform } from "./index";
import * as protos from "@dataform/protos";
import * as parser from "./parser";

export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type MaterializationType = "table" | "view" | "incremental";

export interface MConfig {
  type: MaterializationType;
  query: MContextable<string>;
  where: MContextable<string>;
  pre: MContextable<string | string[]>;
  post: MContextable<string | string[]>;
  assert: MContextable<string | string[]>;
  dependencies: string | string[];
  schema: { [key: string]: string } | string[];
}

export class Materialization {
  proto: protos.Materialization = protos.Materialization.create({
    type: "view"
  });

  // Hold a reference to the Dataform instance.
  dataform: Dataform;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: MContextable<string>;
  private contextableWhere: MContextable<string>;
  private contextablePres: MContextable<string | string[]>[] = [];
  private contextablePosts: MContextable<string | string[]>[] = [];
  private contextableAssertions: MContextable<string | string[]>[] = [];

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
    if (config.pre) {
      this.pre(config.pre);
    }
    if (config.post) {
      this.post(config.post);
    }
    if (config.assert) {
      this.assert(config.assert);
    }
    if (config.dependencies) {
      this.dependency(config.dependencies);
    }
    if (config.schema) {
      this.schema(config.schema as any);
    }
  }

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
    this.contextablePres.push(pres);
    return this;
  }

  public post(posts: MContextable<string | string[]>) {
    this.contextablePosts.push(posts);
    return this;
  }

  public assert(query: MContextable<string | string[]>) {
    this.contextableAssertions.push(query);
    return this;
  }

  public dependency(value: string | string[]) {
    var newDependencies = typeof value === "string" ? [value] : value;
    newDependencies.forEach(d => {
      if (this.proto.dependencies.indexOf(d) < 0) {
        this.proto.dependencies.push(d);
      }
    });
    return this;
  }

  public schema(key: string, description?: string);
  public schema(map: { [key: string]: string });
  public schema(keys: string[]);
  public schema(
    keyOrKeysOrMap: string | string[] | { [key: string]: string },
    description?: string
  ) {
    if (!!this.proto.schema) {
      this.proto.schema = {};
    }
    if (typeof keyOrKeysOrMap === "string") {
      this.proto.schema[keyOrKeysOrMap] = description || "";
    } else if (keyOrKeysOrMap instanceof Array) {
      keyOrKeysOrMap.forEach(key => {
        this.proto.schema[key] = "";
      });
    } else {
      Object.keys(keyOrKeysOrMap).forEach(key => {
        this.proto.schema[key] = keyOrKeysOrMap[key] || "";
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

    this.contextablePres.forEach(contextablePres => {
      var appliedPres = context.apply(contextablePres);
      this.proto.pres = (this.proto.pres || []).concat(
        typeof appliedPres == "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePres = [];

    this.contextablePosts.forEach(contextablePosts => {
      var appliedPosts = context.apply(contextablePosts);
      this.proto.posts = (this.proto.posts || []).concat(
        typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePosts = [];

    this.contextableAssertions.forEach(contextableAssertions => {
      var appliedAssertions = context.apply(contextableAssertions);
      this.proto.assertions = (this.proto.assertions || []).concat(
        typeof appliedAssertions == "string"
          ? [appliedAssertions]
          : appliedAssertions
      );
    });
    this.contextableAssertions = [];

    // Compute columns.
    try {
      var tree = parser.parse(this.proto.query, {});
      this.proto.parsedColumns = tree.statement[0].result
        .map(res => res.alias)
        .map(column => column || "*");
    } catch (e) {
      // There was an exception parsing the columns, ignore.
    }
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
    return this.materialization.dataform
      .adapter()
      .resolveTarget(this.materialization.proto.target);
  }

  public ref(name: string) {
    this.materialization.dependency(name);
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

  public pre(statement: MContextable<string | string[]>) {
    this.materialization.pre(statement);
    return "";
  }

  public post(statement: MContextable<string | string[]>) {
    this.materialization.post(statement);
    return "";
  }

  public dependency(name: string) {
    this.materialization.dependency(name);
    return "";
  }

  public assert(query: MContextable<string | string[]>) {
    this.materialization.assert(query);
    return "";
  }

  public schema(key: string, description?: string);
  public schema(map: { [key: string]: string });
  public schema(keys: string[]);
  public schema(
    keyOrKeysOrMap: string | string[] | { [key: string]: string },
    description?: string
  ) {
    this.materialization.schema(keyOrKeysOrMap as any, description);
    return "";
  }

  public describe(key: string, description?: string) {
    this.materialization.schema(key, description);
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
