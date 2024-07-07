import { Assertion } from "df/core/actions/assertion";
import { Declaration } from "df/core/actions/declaration";
import { ILegacyIncrementalTableConfig, IncrementalTable } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { ILegacyTableConfig, Table } from "df/core/actions/table";
import { ILegacyViewConfig, View } from "df/core/actions/view";
import { ICommonContext } from "df/core/common";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

export type Action =
  | Table
  | View
  | IncrementalTable
  | Operation
  | Assertion
  | Declaration
  | Notebook;

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

/**
 * Context methods are available when evaluating contextable SQL code, such as
 * within SQLX files, or when using a [Contextable](#Contextable) argument with the JS API.
 */
export interface ITableContext extends ICommonContext {
  /**
   * Shorthand for an `if` condition. Equivalent to `cond ? trueCase : falseCase`.
   * `falseCase` is optional, and defaults to an empty string.
   */
  when: (cond: boolean, trueCase: string, falseCase?: string) => string;

  /**
   * Indicates whether the config indicates the file is dealing with an incremental table.
   */
  incremental: () => boolean;
}

export class LegacyConfigConverter {
  // This is a workaround to make bigquery options output empty fields with the same behaviour as
  // they did previously.
  public static legacyConvertBigQueryOptions(bigquery: dataform.IBigQueryOptions) {
    let bigqueryFiltered: dataform.IBigQueryOptions = {};
    Object.entries(bigquery).forEach(([key, value]) => {
      if (value) {
        bigqueryFiltered = {
          ...bigqueryFiltered,
          [key]: value
        };
      }
    });
    return bigqueryFiltered;
  }

  public static insertLegacyInlineAssertionsToConfigProto(
    legacyConfig: ILegacyTableConfig | ILegacyIncrementalTableConfig | ILegacyViewConfig
  ) {
    if (legacyConfig?.assertions) {
      if (legacyConfig.assertions.uniqueKey) {
        legacyConfig.assertions.uniqueKey = legacyConfig.assertions.uniqueKey;
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

  public static insertLegacyBigQueryOptionsToConfigProto(
    legacyConfig: ILegacyTableConfig | ILegacyIncrementalTableConfig
  ) {
    if (legacyConfig?.bigquery) {
      if (!!legacyConfig.bigquery.partitionBy) {
        legacyConfig.partitionBy = legacyConfig.bigquery.partitionBy;
      }
      if (!!legacyConfig.bigquery.clusterBy) {
        legacyConfig.clusterBy = legacyConfig.bigquery.clusterBy;
      }
      if (!!legacyConfig.bigquery.updatePartitionFilter) {
        legacyConfig.updatePartitionFilter = legacyConfig.bigquery.updatePartitionFilter;
      }
      if (!!legacyConfig.bigquery.labels) {
        legacyConfig.labels = legacyConfig.bigquery.labels;
      }
      if (!!legacyConfig.bigquery.partitionExpirationDays) {
        legacyConfig.partitionExpirationDays = legacyConfig.bigquery.partitionExpirationDays;
      }
      if (!!legacyConfig.bigquery.requirePartitionFilter) {
        legacyConfig.requirePartitionFilter = legacyConfig.bigquery.requirePartitionFilter;
      }
      if (!!legacyConfig.bigquery.additionalOptions) {
        legacyConfig.additionalOptions = legacyConfig.bigquery.additionalOptions;
      }
      delete legacyConfig.bigquery;
    }
    return legacyConfig;
  }
}

export interface ILegacyTableBigqueryConfig {
  partitionBy?: string;
  clusterBy?: string[];
  updatePartitionFilter?: string;
  labels?: { [key: string]: string };
  partitionExpirationDays?: number;
  requirePartitionFilter?: boolean;
  additionalOptions?: { [key: string]: string };
}
