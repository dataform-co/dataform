import { QueryOrAction } from "df/cli/api/dbadapters/execution_sql";
import { dataform } from "df/protos/ts";

export type OnCancel = (handleCancel: () => void) => void;

export interface IExecutionResult {
  rows: any[];
  metadata: dataform.IExecutionMetadata;
}

export interface IExecutionResultRaw extends IExecutionResult {
  schema?: dataform.IField[];
}

export interface IBigQueryError extends Error {
  metadata?: dataform.IExecutionMetadata
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
        dryRun?: boolean;
        reservation?: string;
      };
    }
  ): Promise<IExecutionResult>;

  executeRaw(
    statement: string,
    options?: {
      params?: { [name: string]: any };
      rowLimit?: number;
      bigquery?: {
        labels?: { [label: string]: string };
        location?: string;
        jobPrefix?: string;
        dryRun?: boolean;
        reservation?: string;
      };
    }
  ): Promise<IExecutionResultRaw>;
}

export interface IDbAdapter extends IDbClient {
  withClientLock<T>(callback: (client: IDbClient) => Promise<T>): Promise<T>;

  evaluate(queryOrAction: QueryOrAction): Promise<dataform.IQueryEvaluation[]>;

  schemas(database: string): Promise<string[]>;
  createSchema(database: string, schema: string): Promise<void>;

  tables(database: string, schema?: string): Promise<dataform.ITableMetadata[]>;
  search(searchText: string, options?: { limit: number }): Promise<dataform.ITableMetadata[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  deleteTable(target: dataform.ITarget): Promise<void>;

  setMetadata(action: dataform.IExecutionAction): Promise<void>;
}
