import { Dataform } from "./index";
import * as protos from "./protos";
import * as parser from "./parser";

export type MContextable<T> = T | ((ctx: MaterializationContext) => T);
export type MaterializationType = "table" | "view" | "incremental";

export class Materialization {
  proto: protos.Materialization = protos.Materialization.create({
    type: "view"
  });

  // Hold a reference to the Dataform instance.
  dataform: Dataform;

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
    return this.proto;
  }
}

export class MaterializationContext {
  private materialization?: Materialization;

  constructor(materialization: Materialization) {
    this.materialization = materialization;
  }

  public self(): string {
    return this.materialization.dataform
      .adapter()
      .queryableName(this.materialization.proto.target);
  }

  public ref(name: string) {
    this.materialization.proto.dependencies.push(name);
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
