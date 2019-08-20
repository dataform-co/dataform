import { Credentials } from "@dataform/api/commands/credentials";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import { RedshiftDbAdapter } from "@dataform/api/dbadapters/redshift";
import { SnowflakeDbAdapter } from "@dataform/api/dbadapters/snowflake";
import { SQLDataWarehouseDBAdapter } from "@dataform/api/dbadapters/sqldatawarehouse";
import { dataform } from "@dataform/protos";

export type OnCancel = (handleCancel: () => void) => void;

export interface IDbAdapter {
  execute(
    statement: string,
    options?: {
      onCancel?: OnCancel;
      interactive?: boolean;
    }
  ): Promise<any[]>;
  evaluate(statement: string): Promise<void>;
  tables(): Promise<dataform.ITarget[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  preview(target: dataform.ITarget, limitRows?: number): Promise<any[]>;
  prepareSchema(schema: string): Promise<void>;
}

export type DbAdapterConstructor<T extends IDbAdapter> = new (credentials: Credentials) => T;

const registry: { [warehouseType: string]: DbAdapterConstructor<IDbAdapter> } = {};

export function register(warehouseType: string, c: DbAdapterConstructor<IDbAdapter>) {
  registry[warehouseType] = c;
}

export function create(credentials: Credentials, warehouseType: string): IDbAdapter {
  if (!registry[warehouseType]) {
    throw Error(`Unsupported warehouse: ${warehouseType}`);
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
