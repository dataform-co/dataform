import { Assertion } from "df/core/actions/assertion";
import { Declaration } from "df/core/actions/declaration";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

export type Action = Table | Operation | Assertion | Declaration | Notebook;

// TODO(ekrekr): In v4, make all method on inheritors of this private, forcing users to use
// constructors in order to populate actions.
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
    useDefaultAssertionDataset = false
  ): dataform.Target {
    const defaultSchema = useDefaultAssertionDataset
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

  /** Retrieves the filename from the config. */
  public abstract getFileName(): string;

  /** Retrieves the resolved target from the proto. */
  public abstract getTarget(): dataform.Target;

  /** Creates the final protobuf representation. */
  public abstract compile(): T;

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
}
