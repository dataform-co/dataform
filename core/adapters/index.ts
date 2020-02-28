import { BigQueryAdapter } from "@dataform/core/adapters/bigquery";
import { RedshiftAdapter } from "@dataform/core/adapters/redshift";
import { SnowflakeAdapter } from "@dataform/core/adapters/snowflake";
import { SQLDataWarehouseAdapter } from "@dataform/core/adapters/sqldatawarehouse";
import { Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";

export interface IAdapter {
  resolveTarget(target: dataform.ITarget): string;
  normalizeIdentifier(identifier: string): string;

  sqlString(stringContents: string): string;

  publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks;
  assertTasks(assertion: dataform.IAssertion, projectConfig: dataform.IProjectConfig): Tasks;

  dropIfExists(target: dataform.ITarget, type: string): string;
  baseTableType(type: string): string;

  indexAssertion(dataset: string, indexCols: string[]): string;
  rowConditionsAssertion(dataset: string, rowConditions: string[]): string;
}

export type AdapterConstructor<T extends IAdapter> = new (
  projectConfig: dataform.IProjectConfig,
  dataformCoreVersion: string
) => T;

export enum WarehouseType {
  BIGQUERY = "bigquery",
  POSTGRES = "postgres",
  REDSHIFT = "redshift",
  SNOWFLAKE = "snowflake",
  SQLDATAWAREHOUSE = "sqldatawarehouse"
}

const CANCELLATION_SUPPORTED = [WarehouseType.BIGQUERY, WarehouseType.SQLDATAWAREHOUSE];

export function supportsCancel(warehouseType: WarehouseType) {
  return CANCELLATION_SUPPORTED.some(w => {
    return w === warehouseType;
  });
}

const requiredBigQueryWarehouseProps: Array<keyof dataform.IBigQuery> = [
  "projectId",
  "credentials"
];
const requiredJdbcWarehouseProps: Array<keyof dataform.IJDBC> = [
  "host",
  "port",
  "username",
  "password",
  "databaseName"
];
const requiredSnowflakeWarehouseProps: Array<keyof dataform.ISnowflake> = [
  "accountId",
  "username",
  "password",
  "role",
  "databaseName",
  "warehouse"
];
const requiredSQLDataWarehouseProps: Array<keyof dataform.ISQLDataWarehouse> = [
  "server",
  "port",
  "username",
  "password",
  "database"
];

export const requiredWarehouseProps = {
  [WarehouseType.BIGQUERY]: requiredBigQueryWarehouseProps,
  [WarehouseType.POSTGRES]: requiredJdbcWarehouseProps,
  [WarehouseType.REDSHIFT]: requiredJdbcWarehouseProps,
  [WarehouseType.SNOWFLAKE]: requiredSnowflakeWarehouseProps,
  [WarehouseType.SQLDATAWAREHOUSE]: requiredSQLDataWarehouseProps
};

const registry: { [warehouseType: string]: AdapterConstructor<IAdapter> } = {};

export function register(warehouseType: string, c: AdapterConstructor<IAdapter>) {
  registry[warehouseType] = c;
}

export function create(
  projectConfig: dataform.IProjectConfig,
  dataformCoreVersion: string
): IAdapter {
  if (!registry[projectConfig.warehouse]) {
    throw new Error(`Unsupported warehouse: ${projectConfig.warehouse}`);
  }
  return new registry[projectConfig.warehouse](projectConfig, dataformCoreVersion);
}

register("bigquery", BigQueryAdapter);
// TODO: The redshift client library happens to work well for postgres, but we should probably
// not be relying on that behaviour. At some point we should replace this with a first-class
// PostgresAdapter.
register("postgres", RedshiftAdapter);
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
register("sqldatawarehouse", SQLDataWarehouseAdapter);
