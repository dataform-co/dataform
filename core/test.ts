import { Session } from "@dataform/core/session";
import * as table from "@dataform/core/table";
import { ITableContext } from "@dataform/core/table";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { Resolvable, ICommonConfig, ICommonContext } from "df/core/common";
import { Contextable } from "df/core/contextable";

export interface ITestConfig {
  /**
   * The dataset that this unit test is to be run against.
   */
  dataset?: Resolvable;
}

export interface ITestContext extends ICommonContext {}

/**
 * @hidden
 */
export class Test {
  public proto: dataform.ITest = dataform.Test.create();

  public session: Session;
  public contextableInputs: { [refName: string]: Contextable<ITestContext, string> } = {};

  private datasetToTest: Resolvable;
  private contextableQuery: Contextable<ITestContext, string>;

  public config(config: ITestConfig) {
    if (config.dataset) {
      this.dataset(config.dataset);
    }
    return this;
  }

  public dataset(ref: Resolvable) {
    this.datasetToTest = ref;
    return this;
  }

  public input(refName: string, contextableQuery: Contextable<ITestContext, string>) {
    this.contextableInputs[refName] = contextableQuery;
    return this;
  }

  public expect(contextableQuery: Contextable<ITestContext, string>) {
    this.contextableQuery = contextableQuery;
    return this;
  }

  public compile() {
    const testContext = new TestContext(this);
    if (!this.datasetToTest) {
      this.session.compileError(
        new Error("Tests must operate upon a specified dataset."),
        this.proto.fileName
      );
    } else {
      const allResolved = this.session.findActions(utils.resolvableAsTarget(this.datasetToTest));
      if (allResolved.length > 1) {
        this.session.compileError(
          new Error(utils.ambiguousActionNameMsg(this.datasetToTest, allResolved)),
          this.proto.fileName
        );
      }
      const dataset = allResolved.length > 0 ? allResolved[0] : undefined;
      if (!(dataset && dataset instanceof table.Table)) {
        this.session.compileError(
          new Error(`Dataset ${utils.stringifyResolvable(this.datasetToTest)} could not be found.`),
          this.proto.fileName
        );
      } else if (dataset.proto.type === "incremental") {
        this.session.compileError(
          new Error("Running tests on incremental datasets is not yet supported."),
          this.proto.fileName
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

/**
 * @hidden
 */
export class TestContext {
  public readonly test: Test;
  constructor(test: Test) {
    this.test = test;
  }

  public apply<T>(value: Contextable<ITestContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}

/**
 * @hidden
 */
class RefReplacingContext implements ITableContext {
  private readonly testContext: TestContext;

  constructor(testContext: TestContext) {
    this.testContext = testContext;
  }

  public ref(ref: Resolvable | string[], ...rest: string[]) {
    return this.resolve(ref, ...rest);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    ref = utils.toResolvable(ref, rest);
    if (typeof ref !== "string") {
      this.testContext.test.session.compileError(
        new Error("Tests do not currently support referencing non-string inputs.")
      );
      return "";
    }
    if (!this.testContext.test.contextableInputs[ref]) {
      this.testContext.test.session.compileError(
        new Error(`Input for dataset "${ref}" has not been provided.`)
      );
      return "";
    }
    return `(${this.testContext.apply(this.testContext.test.contextableInputs[ref])})`;
  }

  public apply<T>(value: Contextable<ITableContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }

  public config(config: table.ITableConfig) {
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

  public where(where: Contextable<ITableContext, string>) {
    return "";
  }

  public isIncremental() {
    return false;
  }

  public ifIncremental(value: string) {
    // Use the non-incremental query for unit tests.
    return "";
  }

  public preOps(statement: Contextable<ITableContext, string | string[]>) {
    return "";
  }

  public postOps(statement: Contextable<ITableContext, string | string[]>) {
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
