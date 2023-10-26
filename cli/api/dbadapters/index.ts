import { Credentials } from "df/cli/api/commands/credentials";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { QueryOrAction } from "df/cli/api/dbadapters/execution_sql";
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
        location?: string;
        jobPrefix?: string;
      };
    }
  ): Promise<IExecutionResult>;
}

export interface IDbAdapter extends IDbClient {
  withClientLock<T>(callback: (client: IDbClient) => Promise<T>): Promise<T>;

  evaluate(queryOrAction: QueryOrAction): Promise<dataform.IQueryEvaluation[]>;

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

export const validWarehouses = ["bigquery"];

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
