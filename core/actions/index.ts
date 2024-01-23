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

  constructor(session?: Session) {
    this.session = session;
  }

  // Applying the session canonically means using the schema and database present before overrides.
  public applySessionCanonicallyToTarget(targetFromConfig: dataform.Target): dataform.Target {
    return dataform.Target.create({
      name: targetFromConfig.name,
      schema: targetFromConfig.schema || this.session.canonicalConfig.defaultSchema || undefined,
      database:
        targetFromConfig.database || this.session.canonicalConfig.defaultDatabase || undefined
    });
  }

  public applySessionToTarget(targetFromConfig: dataform.Target): dataform.Target {
    return dataform.Target.create({
      name: targetFromConfig.name,
      schema: targetFromConfig.schema || this.session.config.defaultSchema || undefined,
      database: targetFromConfig.database || this.session.config.defaultDatabase || undefined
    });
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
