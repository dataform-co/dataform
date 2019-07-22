import { dataform } from "@dataform/protos";
import { Tasks } from "../tasks";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";
import { SnowflakeAdapter } from "./snowflake";
import { SQLDataWarehouseAdapter } from "./sqldatawarehouse";

export interface IAdapter {
  resolveTarget(target: dataform.ITarget): string;

  publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks;
  assertTasks(assertion: dataform.IAssertion, projectConfig: dataform.IProjectConfig): Tasks;

  dropIfExists(target: dataform.ITarget, type: string): string;
  baseTableType(type: string): string;
}

export type AdapterConstructor<T extends IAdapter> = new (
  projectConfig: dataform.IProjectConfig
) => T;

export enum WarehouseType {
  BIGQUERY = "bigquery",
  POSTGRES = "postgres",
  REDSHIFT = "redshift",
  SNOWFLAKE = "snowflake",
  SQLDATAWAREHOUSE = "sqldatawarehouse"
}

export function supportsCancel(warehouseType: WarehouseType) {
  if (warehouseType === WarehouseType.BIGQUERY || warehouseType === WarehouseType.SQLDATAWAREHOUSE)
      return true
  else
    return false
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
  "databaseName"
]

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

export function create(projectConfig: dataform.IProjectConfig): IAdapter {
  if (!registry[projectConfig.warehouse]) {
    throw Error(`Unsupported warehouse: ${projectConfig.warehouse}`);
  }
  return new registry[projectConfig.warehouse](projectConfig);
}

register("bigquery", BigQueryAdapter);
// TODO: The redshift client library happens to work well for postgres, but we should probably
// not be relying on that behaviour. At some point we should replace this with a first-class
// PostgresAdapter.
register("postgres", RedshiftAdapter);
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
register("sqldatawarehouse", SQLDataWarehouseAdapter);
