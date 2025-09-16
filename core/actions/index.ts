import { Assertion } from "df/core/actions/assertion";
import { DataPreparation } from "df/core/actions/data_preparation";
import { Declaration } from "df/core/actions/declaration";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { View } from "df/core/actions/view";
import { IColumnsDescriptor } from "df/core/column_descriptors";
import { Resolvable } from "df/core/contextables";
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

export type ActionProto =
  | dataform.Table // core.proto's Table represents the Table, View or IncrementalTable action type.
  | dataform.Operation
  | dataform.Assertion
  | dataform.Declaration
  | dataform.Notebook
  | dataform.DataPreparation;

// In v4, consider making methods on inheritors of this private, forcing users to use constructors
// in order to populate actions.
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
    options?: {
      validateTarget?: boolean;
      useDefaultAssertionDataset?: boolean;
    }
  ): dataform.Target {
    const defaultSchema = options?.useDefaultAssertionDataset
      ? projectConfig.assertionSchema || projectConfig.defaultSchema
      : projectConfig.defaultSchema;
    const target = dataform.Target.create({
      name: targetFromConfig.name,
      schema: targetFromConfig.schema || defaultSchema || undefined,
      database: targetFromConfig.database || projectConfig.defaultDatabase || undefined
    });
    if (options?.validateTarget) {
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

  protected generateInlineAssertions(
    tableAssertionsConfig: dataform.ActionConfig.TableAssertionsConfig,
    proto: dataform.Table
  ): { uniqueKeyAssertions: Assertion[]; rowConditionsAssertion?: Assertion } {
    const inlineAssertions: {
      uniqueKeyAssertions: Assertion[];
      rowConditionsAssertion?: Assertion;
    } = { uniqueKeyAssertions: [] };
    if (!!tableAssertionsConfig.uniqueKey?.length && !!tableAssertionsConfig.uniqueKeys?.length) {
      this.session.compileError(
        new Error("Specify at most one of 'assertions.uniqueKey' and 'assertions.uniqueKeys'.")
      );
    }
    const assertionPrefix = !!this.session.projectConfig.builtinAssertionNamePrefix ? `${this.session.projectConfig.builtinAssertionNamePrefix}_` : "";
    let uniqueKeys = tableAssertionsConfig.uniqueKeys.map(uniqueKey =>
      dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create(uniqueKey)
    );
    if (!!tableAssertionsConfig.uniqueKey?.length) {
      uniqueKeys = [
        dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create({
          uniqueKey: tableAssertionsConfig.uniqueKey
        })
      ];
    }
    if (uniqueKeys) {
      uniqueKeys.forEach(({ uniqueKey }, index) => {
        const uniqueKeyAssertion = this.session
          .assert(
            `${assertionPrefix}${proto.target.schema}_${proto.target.name}_assertions_uniqueKey_${index}`,
            dataform.ActionConfig.AssertionConfig.create({ filename: proto.fileName })
          )
          .query(ctx =>
            this.session.compilationSql().indexAssertion(ctx.ref(proto.target), uniqueKey)
          );
        if (proto.tags) {
          uniqueKeyAssertion.tags(proto.tags);
        }
        uniqueKeyAssertion.setParentAction(dataform.Target.create(proto.target));
        if (proto.disabled) {
          uniqueKeyAssertion.disabled();
        }
        inlineAssertions.uniqueKeyAssertions.push(uniqueKeyAssertion);
      });
    }
    const mergedRowConditions = tableAssertionsConfig.rowConditions || [];
    if (!!tableAssertionsConfig.nonNull) {
      const nonNullCols =
        typeof tableAssertionsConfig.nonNull === "string"
          ? [tableAssertionsConfig.nonNull]
          : tableAssertionsConfig.nonNull;
      nonNullCols.forEach(nonNullCol => mergedRowConditions.push(`${nonNullCol} IS NOT NULL`));
    }
    if (!!mergedRowConditions && mergedRowConditions.length > 0) {
      inlineAssertions.rowConditionsAssertion = this.session
        .assert(`${assertionPrefix}${proto.target.schema}_${proto.target.name}_assertions_rowConditions`, {
          filename: proto.fileName
        } as dataform.ActionConfig.AssertionConfig)
        .query(ctx =>
          this.session
            .compilationSql()
            .rowConditionsAssertion(ctx.ref(proto.target), mergedRowConditions)
        );
      inlineAssertions.rowConditionsAssertion.setParentAction(dataform.Target.create(proto.target));
      if (proto.disabled) {
        inlineAssertions.rowConditionsAssertion.disabled();
      }
      if (proto.tags) {
        inlineAssertions.rowConditionsAssertion.tags(proto.tags);
      }
    }
    return inlineAssertions;
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
}

export function checkConfigAdditionalOptionsOverlap(
  config: dataform.ActionConfig.TableConfig | dataform.ActionConfig.IncrementalTableConfig,
  session: Session
) {
  const target = dataform.Target.create({
    database: config.project,
    schema: config.dataset,
    name: config.name
  });
  if (config.partitionExpirationDays && config.additionalOptions.partition_expiration_days) {
    session.compileError(
      `partitionExpirationDays has been declared twice`,
      config.filename,
      target
    );
  }
  if (config.requirePartitionFilter && config.additionalOptions.require_partition_filter) {
    session.compileError(`requirePartitionFilter has been declared twice`, config.filename, target);
  }
}

/**
 * @hidden
 * @deprecated
 * Use core.proto config options instead.
 */
export interface INamedConfig {
  /**
   * The type of the action.
   *
   * @hidden
   */
  type?: string;

  /**
   * The name of the action.
   *
   * @hidden
   */
  name?: string;
}

/**
 * @hidden
 * @deprecated
 * Use core.proto config options instead.
 */
export interface IActionConfig {
  /**
   * A list of user-defined tags with which the action should be labeled.
   */
  tags?: string[];

  /**
   * Dependencies of the action.
   *
   * @hidden
   */
  dependencies?: Resolvable | Resolvable[];

  /**
   * If set to true, this action will not be executed. However, the action may still be depended upon.
   * Useful for temporarily turning off broken actions.
   */
  disabled?: boolean;
}

/**
 * @hidden
 * @deprecated
 * Use core.proto config options instead.
 */
export interface ITargetableConfig {
  /**
   * The database in which the output of this action should be created.
   */
  database?: string;

  /**
   * The schema in which the output of this action should be created.
   */
  schema?: string;
}

/**
 * @hidden
 * @deprecated
 * Use core.proto config options instead.
 */
export interface IDependenciesConfig {
  /**
   * One or more explicit dependencies for this action. Dependency actions will run before dependent actions.
   * Typically this would remain unset, because most dependencies are declared as a by-product of using the `ref` function.
   */
  dependencies?: Resolvable | Resolvable[];

  /**
   * Declares whether or not this action is hermetic. An action is hermetic if all of its dependencies are explicitly
   * declared.
   *
   * If this action depends on data from a source which has not been declared as a dependency, then `hermetic`
   * should be explicitly set to `false`. Otherwise, if this action only depends on data from explicitly-declared
   * dependencies, then it should be set to `true`.
   */
  hermetic?: boolean;

  /**
   * If this flag is set to true, assertions dependent upon any of the dependencies are added as dependencies as well.
   */
  dependOnDependencyAssertions?: boolean;
}

/**
 * @hidden
 * @deprecated
 * Use core.proto config options instead.
 */
export interface IDocumentableConfig {
  /**
   * A description of columns within the dataset.
   */
  columns?: IColumnsDescriptor;

  /**
   * A description of the dataset.
   */
  description?: string;
}

/**
 * @hidden
 * @deprecated
 * This is no longer needed other than for legacy backwards compatibility purposes, as tables are
 * now configured in separate actions.
 */
export type TableType = typeof TableType[number];

/**
 * @hidden
 * @deprecated
 * This is no longer needed other than for legacy backwards compatibility purposes, as tables are
 * now configured in separate actions.
 */
export const TableType = ["table", "view", "incremental"] as const;

/**
 * @hidden
 * @deprecated
 * These options are only here to preserve backwards compatibility of legacy config options.
 * consider breaking backwards compatability of this in v4.
 */
export interface ILegacyTableConfig
  extends IActionConfig,
    IDependenciesConfig,
    IDocumentableConfig,
    INamedConfig,
    ITargetableConfig {
  type?: TableType;
  protected?: boolean;
  bigquery?: ILegacyBigQueryOptions;
  assertions?: ILegacyTableAssertions;
  uniqueKey?: string[];
  materialized?: boolean;
}

/**
 * @hidden
 * @deprecated
 * These options are only here to preserve backwards compatibility of legacy config options.
 * consider breaking backwards compatability of this in v4.
 */
export interface ILegacyBigQueryOptions {
  partitionBy?: string;
  clusterBy?: string[];
  updatePartitionFilter?: string;
  labels?: { [name: string]: string };
  partitionExpirationDays?: number;
  requirePartitionFilter?: boolean;
  additionalOptions?: { [name: string]: string };
  iceberg?: {
    fileFormat?: string;
    tableFormat?: string;
    connection?: string;
    bucketName?: string;
    tableFolderRoot?: string;
    tableFolderSubpath?: string;
  }
}

/**
 * @hidden
 * @deprecated
 * These options are only here to preserve backwards compatibility of legacy config options.
 * consider breaking backwards compatability of this in v4.
 */
export interface ILegacyTableAssertions {
  uniqueKey?: string | string[];
  uniqueKeys?: string[][];
  nonNull?: string | string[];
  rowConditions?: string[];
}

export class LegacyConfigConverter {
  // This is a workaround to make bigquery options output empty fields with the same behaviour as
  // they did previously.
  public static legacyConvertBigQueryOptions(
    bigquery: dataform.IBigQueryOptions
  ): dataform.IBigQueryOptions {
    let bigqueryFiltered: dataform.IBigQueryOptions = {};
    Object.entries(bigquery).forEach(([key, value]) => {
      if (Array.isArray(value) && value.length === 0) {
        return;
      } else if (typeof value === "object" && Object.entries(value).length === 0) {
        return;
      }
      if (value) {
        bigqueryFiltered = {
          ...bigqueryFiltered,
          [key]: value
        };
      }
    });
    return bigqueryFiltered;
  }

  public static insertLegacyInlineAssertionsToConfigProto<T extends ILegacyTableConfig>(
    unverifiedConfig: T
  ): T {
    // Type `any` is used here to facilitate the type hacking for legacy compatibility.
    const legacyConfig: any = unverifiedConfig;
    if (legacyConfig?.assertions) {
      if (typeof legacyConfig.assertions?.uniqueKey === "string") {
        legacyConfig.assertions.uniqueKey = [legacyConfig.assertions.uniqueKey];
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

  public static insertLegacyBigQueryOptionsToConfigProto<T extends ILegacyTableConfig>(
    unverifiedConfig: T
  ): T {
    // Type `any` is used here to facilitate the type hacking for legacy compatibility.
    const legacyConfig: any = unverifiedConfig;
    if (!legacyConfig?.bigquery) {
      return legacyConfig;
    }
    if (!!legacyConfig.bigquery.partitionBy) {
      legacyConfig.partitionBy = legacyConfig.bigquery.partitionBy;
      delete legacyConfig.bigquery.partitionBy;
    }
    if (!!legacyConfig.bigquery.clusterBy) {
      legacyConfig.clusterBy = legacyConfig.bigquery.clusterBy;
      delete legacyConfig.bigquery.clusterBy;
    }
    if (!!legacyConfig.bigquery.updatePartitionFilter) {
      legacyConfig.updatePartitionFilter = legacyConfig.bigquery.updatePartitionFilter;
      delete legacyConfig.bigquery.updatePartitionFilter;
    }
    if (!!legacyConfig.bigquery.labels) {
      legacyConfig.labels = legacyConfig.bigquery.labels;
      delete legacyConfig.bigquery.labels;
    }
    if (!!legacyConfig.bigquery.partitionExpirationDays) {
      legacyConfig.partitionExpirationDays = legacyConfig.bigquery.partitionExpirationDays;
      delete legacyConfig.bigquery.partitionExpirationDays;
    }
    if (!!legacyConfig.bigquery.requirePartitionFilter) {
      legacyConfig.requirePartitionFilter = legacyConfig.bigquery.requirePartitionFilter;
      delete legacyConfig.bigquery.requirePartitionFilter;
    }
    if (!!legacyConfig.bigquery.additionalOptions) {
      legacyConfig.additionalOptions = legacyConfig.bigquery.additionalOptions;
      delete legacyConfig.bigquery.additionalOptions;
    }
    if(!!legacyConfig.bigquery.iceberg) {
      legacyConfig.iceberg = legacyConfig.bigquery.iceberg;
      delete legacyConfig.bigquery.iceberg;
    }
    // To prevent skipping throwing an error when there are additional, unused fields, only delete
    // the legacy bigquery object if there are no more fields left on it.
    if (Object.keys(legacyConfig.bigquery).length === 0) {
      delete legacyConfig.bigquery;
    }
    return legacyConfig;
  }
}
