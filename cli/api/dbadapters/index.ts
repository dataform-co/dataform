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

  setMetadata(action: dataform.IExecutionAction): Promise<void>;
}
