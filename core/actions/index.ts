import { Assertion, IAssertionConfig } from "df/core/actions/assertion";
import { Declaration, IDeclarationConfig } from "df/core/actions/declaration";
import { Notebook } from "df/core/actions/notebook";
import { IOperationConfig, Operation } from "df/core/actions/operation";
import { ITableConfig, Table, TableType } from "df/core/actions/table";
import { ITestConfig } from "df/core/actions/test";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

export type Action = Table | Operation | Assertion | Declaration | Notebook;

/**
 * @deprecated
 * Configs are soon to be replaced with pure protobuf representations.
 */
export type SqlxConfig = (
  | (ITableConfig & { type: TableType })
  | (IAssertionConfig & { type: "assertion" })
  | (IOperationConfig & { type: "operations" })
  | (IDeclarationConfig & { type: "declaration" })
  | (ITestConfig & { type: "test" })
) & { name: string };

export abstract class ActionBuilder<T> {
  public session: Session;
  public includeAssertionsForDependency: Map<string, boolean> = new Map();

  constructor(session?: Session) {
    this.session = session;
  }

  public applySessionToTarget(
    targetFromConfig: dataform.Target,
    projectConfig: dataform.ProjectConfig,
    fileName?: string,
    validateTarget = false,
    useDefaultAssertionSchema = false
  ): dataform.Target {
    const defaultSchema = useDefaultAssertionSchema
      ? projectConfig.assertionSchema
      : projectConfig.defaultSchema;
    const target = dataform.Target.create({
      name: targetFromConfig.name,
      schema: targetFromConfig.schema || defaultSchema || undefined,
      database: targetFromConfig.database || projectConfig.defaultDatabase || undefined
    });
    if (validateTarget) {
      this.validateTarget(targetFromConfig, fileName);
    }
    return target;
  }

  private validateTarget(target: dataform.Target, fileName: string) {
    if (target.name.includes(".")) {
      this.session.compileError(
        new Error("Action target names cannot include '.'"),
        fileName,
        target
      );
    }
    if (target.schema.includes(".")) {
      this.session.compileError(
        new Error("Action target datasets cannot include '.'"),
        fileName,
        target
      );
    }
    if (target.database.includes(".")) {
      this.session.compileError(
        new Error("Action target projects cannot include '.'"),
        fileName,
        target
      );
    }
  }

  /**
   * @deprecated
   * Configs are soon to be replaced with pure protobuf representations.
   */
  public abstract config(config: any): ActionBuilder<T>;

  /** Retrieves the filename from the config. */
  public abstract getFileName(): string;

  /** Retrieves the resolved target from the proto. */
  public abstract getTarget(): dataform.Target;

  /** Creates the final protobuf representation. */
  public abstract compile(): T;
}
