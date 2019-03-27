import { dataform } from "@dataform/protos";
import { Tasks } from "../tasks";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";
import { SnowflakeAdapter } from "./snowflake";

export interface IAdapter {
  resolveTarget(target: dataform.ITarget): string;

  publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks;
  assertTasks(assertion: dataform.IAssertion, projectConfig: dataform.IProjectConfig): Tasks;

  dropIfExists(target: dataform.ITarget, type: string): string;
}

export type AdapterConstructor<T extends IAdapter> = new (
  projectConfig: dataform.IProjectConfig
) => T;

export enum WarehouseTypes {
  BIGQUERY = "bigquery",
  REDSHIFT = "redshift",
  SNOWFLAKE = "snowflake"
}
export const requiredWarehouseProps = {
  [WarehouseTypes.BIGQUERY]: ["projectId", "credentials"],
  [WarehouseTypes.REDSHIFT]: ["host", "port", "user", "password", "database"],
  [WarehouseTypes.SNOWFLAKE]: [
    "accountId",
    "userName",
    "password",
    "role",
    "databaseName",
    "warehouse"
  ]
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
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
