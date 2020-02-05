import { Credentials } from "@dataform/api/commands/credentials";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import { RedshiftDbAdapter } from "@dataform/api/dbadapters/redshift";
import { SnowflakeDbAdapter } from "@dataform/api/dbadapters/snowflake";
import { SQLDataWarehouseDBAdapter } from "@dataform/api/dbadapters/sqldatawarehouse";
import { dataform } from "@dataform/protos";

export type OnCancel = (handleCancel: () => void) => void;

export interface IExecutionResult {
  rows: any[];
  metadata: dataform.IExecutionMetadata;
}

export interface IDbAdapter {
  execute(
    statement: string,
    options?: {
      onCancel?: OnCancel;
      interactive?: boolean;
      maxResults?: number;
    }
  ): Promise<IExecutionResult>;
  evaluate(statement: string): Promise<dataform.IQueryEvaluationResponse>;
  tables(): Promise<dataform.ITarget[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  preview(target: dataform.ITarget, limitRows?: number): Promise<any[]>;
  prepareSchema(database: string, schema: string): Promise<void>;
  close(): Promise<void>;
}

export type DbAdapterConstructor<T extends IDbAdapter> = new (credentials: Credentials) => T;

const registry: { [warehouseType: string]: DbAdapterConstructor<IDbAdapter> } = {};

export function register(warehouseType: string, c: DbAdapterConstructor<IDbAdapter>) {
  registry[warehouseType] = c;
}

export function create(credentials: Credentials, warehouseType: string): IDbAdapter {
  if (!registry[warehouseType]) {
    throw new Error(`Unsupported warehouse: ${warehouseType}`);
  }
  return new registry[warehouseType](credentials);
}

register("bigquery", BigQueryDbAdapter);
// TODO: The redshift client library happens to work well for postgres, but we should probably
// not be relying on that behaviour. At some point we should replace this with a first-class
// PostgresAdapter.
register("postgres", RedshiftDbAdapter);
register("redshift", RedshiftDbAdapter);
register("snowflake", SnowflakeDbAdapter);
register("sqldatawarehouse", SQLDataWarehouseDBAdapter);
