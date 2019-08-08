import { Session } from "@dataform/core/session";
import * as table from "@dataform/core/table";
import { dataform } from "@dataform/protos";

export type TContextable<T> = T | ((ctx: TestContext) => T);

export interface TConfig {
  dataset?: string;
}

export class Test {
  public proto: dataform.ITest = dataform.Test.create();

  public session: Session;
  public contextableInputs: { [refName: string]: TContextable<string> } = {};

  private datasetToTest: string;
  private contextableQuery: TContextable<string>;

  public config(config: TConfig) {
    if (config.dataset) {
      this.dataset(config.dataset);
    }
    return this;
  }

  public dataset(datasetToTest: string) {
    this.datasetToTest = datasetToTest;
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
    const testContext = new TestContext(this);
    if (!this.datasetToTest) {
      this.session.compileError(new Error("Tests must operate upon a specified dataset."));
    } else {
      const dataset = this.session.tables[this.datasetToTest];
      if (!dataset) {
        this.session.compileError(new Error(`Dataset ${this.datasetToTest} could not be found.`));
      } else if (dataset.proto.type === "incremental") {
        this.session.compileError(
          new Error("Running tests on incremental datasets is not yet supported.")
        );
      } else {
        const refReplacingContext = new RefReplacingContext(testContext);
        this.proto.testQuery = refReplacingContext.apply(dataset.contextableQuery);
      }
    }
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
  private readonly testContext: TestContext;

  constructor(testContext: TestContext) {
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

  public name() {
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

  public tags(tags: string[]) {
    return "";
  }
}
