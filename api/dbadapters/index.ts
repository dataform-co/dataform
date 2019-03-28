import { dataform } from "@dataform/protos";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import { RedshiftDbAdapter } from "@dataform/api/dbadapters/redshift";
import { SnowflakeDbAdapter } from "@dataform/api/dbadapters/snowflake";
import * as util from "@dataform/api/utils";

export type OnCancel = ((handleCancel: () => void) => void);

export interface DbAdapter {
  execute(statement: string, onCancel?: OnCancel): Promise<any[]>;
  evaluate(statement: string): Promise<void>;
  tables(): Promise<dataform.ITarget[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  prepareSchema(schema: string): Promise<void>;
}

export type DbAdapterConstructor<T extends DbAdapter> = new (credentials: util.Credentials) => T;

const registry: { [warehouseType: string]: DbAdapterConstructor<DbAdapter> } = {};

export function register(warehouseType: string, c: DbAdapterConstructor<DbAdapter>) {
  registry[warehouseType] = c;
}

export function create(credentials: util.Credentials, warehouseType?: string): DbAdapter {
  if (warehouseType) {
    if (!registry[warehouseType]) {
      throw Error(`Unsupported warehouse: ${warehouseType}`);
    }
    return new registry[warehouseType](credentials);
  }
  if (credentials instanceof dataform.BigQuery) {
    return new registry.bigquery(credentials);
  }
  if (credentials instanceof dataform.JDBC) {
    return new registry.redshift(credentials);
  }
  if (credentials instanceof dataform.Snowflake) {
    return new registry.snowflake(credentials);
  }
  throw Error("Invalid credentials.");
}

register("bigquery", BigQueryDbAdapter);
register("redshift", RedshiftDbAdapter);
register("snowflake", SnowflakeDbAdapter);
