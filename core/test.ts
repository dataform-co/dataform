import { Session } from "@dataform/core/session";
import * as table from "@dataform/core/table";
import { dataform } from "@dataform/protos";

export type TContextable<T> = T | ((ctx: TestContext) => T);

export class Test {
  public proto: dataform.ITest = dataform.Test.create();

  public session: Session;
  public contextableInputs: { [refName: string]: TContextable<string> } = {};

  private datasetUnderTest: string;
  private contextableQuery: TContextable<string>;

  public dataset(datasetUnderTest: string) {
    this.datasetUnderTest = datasetUnderTest;
    return this;
  }

  public input(refName: string, contextableQuery: TContextable<string>) {
    this.contextableInputs[refName] = contextableQuery;
    return this;
  }

  public expect(contextableQuery: TContextable<string>) {
    this.contextableQuery = contextableQuery;
    return this;
  }

  public compile() {
    if (!this.datasetUnderTest) {
      this.session.compileError(new Error("Tests must operate upon a specified dataset."));
      return;
    }
    const dataset = this.session.tables[this.datasetUnderTest];
    if (!dataset) {
      this.session.compileError(new Error(`Dataset ${this.datasetUnderTest} could not be found.`));
      return;
    }
    if (dataset.proto.type === "incremental") {
      this.session.compileError(
        new Error(`Running tests on incremental datasets is not yet supported.`)
      );
      return;
    }
    const testContext = new TestContext(this);
    const refReplacingContext = new RefReplacingContext(dataset, testContext);
    this.proto.testQuery = refReplacingContext.apply(dataset.contextableQuery);
    this.proto.expectedOutputQuery = testContext.apply(this.contextableQuery);
    return this.proto;
  }
}

export class TestContext {
  public readonly test: Test;
  constructor(test: Test) {
    this.test = test;
  }

  public apply<T>(value: TContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}

class RefReplacingContext implements table.ITableContext {
  private readonly dataset: table.Table;
  private readonly testContext: TestContext;

  constructor(dataset: table.Table, testContext: TestContext) {
    this.dataset = dataset;
    this.testContext = testContext;
  }

  public ref(name: string) {
    return this.resolve(name);
  }

  public resolve(name: string) {
    if (!this.testContext.test.contextableInputs[name]) {
      this.testContext.test.session.compileError(
        new Error(`Input for dataset "${name}" has not been provided.`)
      );
      return "";
    }
    return `(${this.testContext.apply(this.testContext.test.contextableInputs[name])})`;
  }

  public apply<T>(value: table.TContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }

  public config(config: table.TConfig) {
    return "";
  }

  public self() {
    return "";
  }

  public type(type: table.TableType) {
    return "";
  }

  public where(where: table.TContextable<string>) {
    return "";
  }

  public preOps(statement: table.TContextable<string | string[]>) {
    return "";
  }

  public postOps(statement: table.TContextable<string | string[]>) {
    return "";
  }

  public disabled() {
    return "";
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    return "";
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    return "";
  }

  public dependencies(name: string) {
    return "";
  }

  public descriptor(
    keyOrKeysOrMap: string | string[] | { [key: string]: string },
    description?: string
  ): string {
    return "";
  }

  public describe(key: string, description?: string) {
    return "";
  }
}
