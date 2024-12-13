import { Assertion } from "df/core/actions/assertion";
import { DataPreparation } from "df/core/actions/data_preparation";
import { Declaration } from "df/core/actions/declaration";
import { IncrementalTable, ILegacyIncrementalTableConfig } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { View, ILegacyViewConfig } from "df/core/actions/view";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

export type Action =
  | Table
  | View
  | IncrementalTable
  | Operation
  | Assertion
  | Declaration
  | Notebook
  | DataPreparation;

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

  public finalizeTarget(targetFromConfig: dataform.Target): dataform.Target {
    return dataform.Target.create({
      name: this.session.finalizeName(targetFromConfig.name),
      schema: targetFromConfig.schema
        ? this.session.finalizeSchema(targetFromConfig.schema)
        : undefined,
      database: targetFromConfig.database
        ? this.session.finalizeDatabase(targetFromConfig.database)
        : undefined
    });
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

export class LegacyConfigConverter {
  public static insertLegacyInlineAssertionsToConfigProto<
    T extends ILegacyIncrementalTableConfig | ILegacyViewConfig
  >(legacyConfig: T): T {
    if (legacyConfig?.assertions) {
      if (!!legacyConfig.assertions.uniqueKey) {
        if (typeof legacyConfig.assertions.uniqueKey === "string") {
          legacyConfig.assertions.uniqueKey = [legacyConfig.assertions.uniqueKey];
        }
      }
      // This determines if the uniqueKeys is of the legacy type.
      if (legacyConfig.assertions.uniqueKeys?.[0]?.length > 0) {
        legacyConfig.assertions.uniqueKeys = (legacyConfig.assertions
          .uniqueKeys as string[][]).map(uniqueKey =>
          dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create({ uniqueKey })
        );
      }
      if (typeof legacyConfig.assertions.nonNull === "string") {
        legacyConfig.assertions.nonNull = [legacyConfig.assertions.nonNull];
      }
    }
    return legacyConfig;
  }
}
