import * as pg from "pg";
import QueryStream from "pg-query-stream";

import { LimitedResultSet } from "df/api/utils/results";
import { dataform } from "df/protos/ts";

const maybeInitializePg = (() => {
  let initialized = false;
  return () => {
    if (!initialized) {
      initialized = true;
      // Decode BigInt types as Numbers, instead of strings.
      // TODO: This will truncate large values, but is consistent with other adapters. We should change these to all use Long.
      pg.types.setTypeParser(20, Number);
    }
  };
})();

export class PgPoolExecutor {
  private pool: pg.Pool;
  constructor(clientConfig: pg.ClientConfig, options?: { concurrencyLimit?: number }) {
    maybeInitializePg();
    this.pool = new pg.Pool({
      ...clientConfig,
      max: options?.concurrencyLimit
    });
    // https://node-postgres.com/api/pool#events
    // Idle clients in the pool are still connected to the remote host and as such can
    // emit errors. If/when they do, they will automatically be removed from the pool,
    // but we still need to handle the error to prevent crashing the process.
    this.pool.on("error", err => {
      // tslint:disable-next-line: no-console
      console.error("pg.Pool idle client error", err.message, err.stack);
    });
  }

  public async execute(
    statement: string,
    options: {
      onCancel?: (handleCancel: () => void) => void;
      rowLimit?: number;
      byteLimit?: number;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ) {
    if (!options?.rowLimit && !options?.byteLimit) {
      const result = await this.pool.query(statement);
      verifyUniqueColumnNames(result.fields);
      return result.rows;
    }
    return await this.withClientLock(client => client.execute(statement, options));
  }

  public async withClientLock<T>(
    callback: (client: {
      execute(
        statement: string,
        options?: {
          onCancel?: (handleCancel: () => void) => void;
          rowLimit?: number;
          byteLimit?: number;
        }
      ): Promise<any[]>;
    }) => Promise<T>
  ) {
    const client = await this.pool.connect();
    try {
      client.on("error", err => {
        // tslint:disable-next-line: no-console
        console.error("pg.Client client error", err.message, err.stack);
        // Errored connections cause issues when released back to the pool. Instead, close the connection
        // by passing the error to release(). https://github.com/dataform-co/dataform/issues/914
        try {
          client.release(err);
        } catch (e) {
          // tslint:disable-next-line: no-console
          console.error("Error thrown when releasing errored pg.Client", e.message, e.stack);
        }
      });

      return await callback({
        execute: async (
          statement: string,
          options: {
            onCancel?: (handleCancel: () => void) => void;
            rowLimit?: number;
            byteLimit?: number;
          } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
        ) => {
          return await new Promise<any[]>((resolve, reject) => {
            const query = client.query(new QueryStream(statement));
            const results = new LimitedResultSet({
              rowLimit: options?.rowLimit,
              byteLimit: options?.byteLimit
            });
            options?.onCancel?.(() => query.destroy(new Error("Query cancelled.")));
            query.on("data", (row: any) => {
              try {
                verifyUniqueColumnNames((query as any).cursor._result.fields);
              } catch (e) {
                // This causes the "error" handler below to fire.
                query.destroy(e);
                return;
              }
              if (!results.push(row)) {
                // The correct way to stop processing data is to close the cursor itself.
                // This results in "end" firing below. https://node-postgres.com/api/cursor#close
                (query as any).cursor.close();
              }
            });
            query.on("error", err => {
              // Errors don't cause "end" to fire, additionally errored connections
              // cause issues when released back to the pool. Instead, close the connection
              // by passing the error to release(). https://github.com/dataform-co/dataform/issues/914
              try {
                client.release(err);
              } catch (e) {
                // tslint:disable-next-line: no-console
                console.error("Error thrown when releasing errored pg.Query", e.message, e.stack);
              }
              reject(err);
            });
            query.on("end", () => {
              resolve(results.rows);
            });
          });
        }
      });
    } finally {
      try {
        client.release();
      } catch (e) {
        // tslint:disable-next-line: no-console
        console.error("Error thrown when releasing ended pg.Client", e.message, e.stack);
      }
    }
  }

  public async close() {
    await this.pool.end();
  }
}

function verifyUniqueColumnNames(fields: pg.FieldDef[]) {
  const colNames = new Set<string>();
  fields.forEach(field => {
    if (colNames.has(field.name)) {
      throw new Error(`Ambiguous column name: ${field.name}`);
    }
    colNames.add(field.name);
  });
}

// See: https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
export function convertFieldType(type: string) {
  switch (String(type).toUpperCase()) {
    case "FLOAT":
    case "FLOAT4":
    case "FLOAT8":
    case "DOUBLE PRECISION":
    case "REAL":
      return dataform.Field.Primitive.FLOAT;
    case "INTEGER":
    case "INT":
    case "INT2":
    case "INT4":
    case "INT8":
    case "BIGINT":
    case "SMALLINT":
      return dataform.Field.Primitive.INTEGER;
    case "DECIMAL":
    case "NUMERIC":
      return dataform.Field.Primitive.NUMERIC;
    case "BOOLEAN":
    case "BOOL":
      return dataform.Field.Primitive.BOOLEAN;
    case "STRING":
    case "VARCHAR":
    case "CHAR":
    case "CHARACTER":
    case "CHARACTER VARYING":
    case "NVARCHAR":
    case "TEXT":
    case "NCHAR":
    case "BPCHAR":
      return dataform.Field.Primitive.STRING;
    case "DATE":
      return dataform.Field.Primitive.DATE;
    case "TIMESTAMP":
    case "TIMESTAMPZ":
    case "TIMESTAMP WITHOUT TIME ZONE":
    case "TIMESTAMP WITH TIME ZONE":
      return dataform.Field.Primitive.TIMESTAMP;
    default:
      return dataform.Field.Primitive.UNKNOWN;
  }
}
