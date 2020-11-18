import { Credentials } from "df/api/commands/credentials";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { PostgresDbAdapter } from "df/api/dbadapters/postgres";
import { PrestoDbAdapter } from "df/api/dbadapters/presto";
import { RedshiftDbAdapter } from "df/api/dbadapters/redshift";
import { SnowflakeDbAdapter } from "df/api/dbadapters/snowflake";
import { SQLDataWarehouseDBAdapter } from "df/api/dbadapters/sqldatawarehouse";
import { QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

export type OnCancel = (handleCancel: () => void) => void;

export interface IExecutionResult {
  rows: any[];
  metadata: dataform.IExecutionMetadata;
}

export interface IDbClient {
  execute(
    statement: string,
    options?: {
      onCancel?: OnCancel;
      interactive?: boolean;
      rowLimit?: number;
      byteLimit?: number;
      bigquery?: {
        labels?: { [label: string]: string };
      };
    }
  ): Promise<IExecutionResult>;
}

export interface IDbAdapter extends IDbClient {
  withClientLock<T>(callback: (client: IDbClient) => Promise<T>): Promise<T>;

  evaluate(
    queryOrAction: QueryOrAction,
    projectConfig?: dataform.IProjectConfig
  ): Promise<dataform.IQueryEvaluation[]>;

  schemas(database: string): Promise<string[]>;
  createSchema(database: string, schema: string): Promise<void>;

  // TODO: This should take parameters to allow for retrieving from a specific database/schema.
  tables(): Promise<dataform.ITarget[]>;
  search(searchText: string, options?: { limit: number }): Promise<dataform.ITableMetadata[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  preview(target: dataform.ITarget, limitRows?: number): Promise<any[]>;

  setMetadata(action: dataform.IExecutionAction): Promise<void>;

  close(): Promise<void>;
}

interface ICredentialsOptions {
  concurrencyLimit?: number;
  disableSslForTestsOnly?: boolean;
}

export interface IDbAdapterClass<T extends IDbAdapter> {
  create: (credentials: Credentials, options: ICredentialsOptions) => Promise<T>;
}

const registry: { [warehouseType: string]: IDbAdapterClass<IDbAdapter> } = {};

export function register(warehouseType: string, c: IDbAdapterClass<IDbAdapter>) {
  registry[warehouseType] = c;
}

export const validWarehouses = [
  "bigquery",
  "postgres",
  "redshift",
  "sqldatawarehouse",
  "snowflake",
  "presto"
];

export async function create(
  credentials: Credentials,
  warehouseType: typeof validWarehouses[number],
  options?: ICredentialsOptions
): Promise<IDbAdapter> {
  if (!registry[warehouseType]) {
    throw new Error(`Unsupported warehouse: ${warehouseType}`);
  }
  return await registry[warehouseType].create(credentials, options);
}

register("bigquery", BigQueryDbAdapter);
register("postgres", PostgresDbAdapter);
register("redshift", RedshiftDbAdapter);
register("snowflake", SnowflakeDbAdapter);
register("sqldatawarehouse", SQLDataWarehouseDBAdapter);
register("presto", PrestoDbAdapter);
