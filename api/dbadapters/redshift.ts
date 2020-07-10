import * as pg from "pg";
import Cursor from "pg-cursor";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, OnCancel } from "df/api/dbadapters/index";
import { SSHTunnelProxy } from "df/api/ssh_tunnel_proxy";
import { parseRedshiftEvalError } from "df/api/utils/error_parsing";
import { ErrorWithCause } from "df/common/errors/errors";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

interface ICursor {
  read: (
    rowCount: number,
    callback: (err: Error, rows: any[], result: pg.QueryResult) => void
  ) => void;
  close: (callback: (err?: Error) => void) => void;
}

interface IRedshiftAdapterOptions {
  sshTunnel?: SSHTunnelProxy;
  warehouseType?: string;
}

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

export class RedshiftDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials, warehouseType: string) {
    maybeInitializePg();
    const jdbcCredentials = credentials as dataform.IJDBC;
    const baseClientConfig: Partial<pg.ClientConfig> = {
      user: jdbcCredentials.username,
      password: jdbcCredentials.password,
      database: jdbcCredentials.databaseName,
      ssl: true
    };
    if (jdbcCredentials.sshTunnel) {
      const sshTunnel = await SSHTunnelProxy.create(jdbcCredentials.sshTunnel, {
        host: jdbcCredentials.host,
        port: jdbcCredentials.port
      });
      const queryExecutor = new PgPoolExecutor({
        ...baseClientConfig,
        host: "127.0.0.1",
        port: sshTunnel.localPort
      });
      return new RedshiftDbAdapter(queryExecutor, { sshTunnel, warehouseType });
    } else {
      const clientConfig: pg.ClientConfig = {
        ...baseClientConfig,
        host: jdbcCredentials.host,
        port: jdbcCredentials.port
      };
      const queryExecutor = new PgPoolExecutor(clientConfig);
      return new RedshiftDbAdapter(queryExecutor, { warehouseType });
    }
  }

  private constructor(
    private readonly queryExecutor: PgPoolExecutor,
    private readonly options: IRedshiftAdapterOptions
  ) {}

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
      includeQueryInError?: boolean;
    } = { maxResults: 1000 }
  ) {
    try {
      const rows = await this.queryExecutor.execute(statement, options);
      return { rows, metadata: {} };
    } catch (e) {
      if (options.includeQueryInError) {
        throw new Error(`Error encountered while running "${statement}": ${e.message}`);
      }
      throw new ErrorWithCause(
        `Error executing ${this.options.warehouseType} query: ${e.message}`,
        e
      );
    }
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(queryOrAction, false, (query: string) =>
      !!query ? `explain ${query}` : ""
    ).map((validationQuery, index) => ({ index, validationQuery }));
    const validationQueriesWithoutWrappers = collectEvaluationQueries(queryOrAction, false);

    const queryEvaluations = new Array<dataform.IQueryEvaluation>();
    for (const { index, validationQuery } of validationQueries) {
      let evaluationResponse: dataform.IQueryEvaluation = {
        status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      };
      try {
        await this.execute(validationQuery.query);
      } catch (e) {
        evaluationResponse = {
          status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
          error: parseRedshiftEvalError(validationQuery.query, e)
        };
      }
      queryEvaluations.push(
        dataform.QueryEvaluation.create({
          ...evaluationResponse,
          incremental: validationQuery.incremental,
          query: validationQueriesWithoutWrappers[index].query
        })
      );
    }
    return queryEvaluations;
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const hasSpectrumTables = await this.hasSpectrumTables();
    const queryResult = await this.execute(
      `select table_name, table_schema
     from information_schema.tables
     where table_schema != 'information_schema'
       and table_schema != 'pg_catalog'
       and table_schema != 'pg_internal'
       ${
         hasSpectrumTables
           ? "union select tablename as table_name, schemaname as table_schema from svv_external_tables"
           : ""
       }`,
      { maxResults: 10000, includeQueryInError: true }
    );
    const { rows } = queryResult;
    return rows.map(row => ({
      schema: row.table_schema,
      name: row.table_name
    }));
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const hasSpectrumTables = await this.hasSpectrumTables();
    const [columnResults, tableResults, externalTableResults] = await Promise.all([
      this.execute(
        `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' and table_name = '${target.name}'
       ${
         hasSpectrumTables
           ? `union
       select columnname as column_name, external_type as data_type, 'not_available' as is_nullable 
       from svv_external_columns 
       where schemaname = '${target.schema}' and tablename = '${target.name}'`
           : ""
       }`,
        { includeQueryInError: true }
      ),
      this.execute(
        `select table_type from information_schema.tables where table_schema = '${target.schema}' and table_name = '${target.name}'`,
        { includeQueryInError: true }
      ),
      hasSpectrumTables
        ? this.execute(
            `select 'TABLE' as table_type from svv_external_tables where schemaname = '${target.schema}' and tablename = '${target.name}'`,
            { includeQueryInError: true }
          )
        : { rows: [], metadata: {} }
    ]);
    const allTableResults = tableResults.rows.concat(externalTableResults.rows);
    if (allTableResults.length > 0) {
      // The table exists.
      return {
        target,
        type: allTableResults[0].table_type === "VIEW" ? "view" : "table",
        typeEnum:
          allTableResults[0].table_type === "VIEW"
            ? dataform.TableMetadata.Type.VIEW
            : dataform.TableMetadata.Type.TABLE,
        fields: columnResults.rows.map(row => ({
          name: row.column_name,
          primitive: row.data_type,
          primitiveEnum: convertFieldType(row.data_type),
          flags: row.is_nullable && row.is_nullable === "YES" ? ["nullable"] : []
        }))
      };
    } else {
      return null;
    }
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`
    );
    return rows;
  }

  public async prepareSchema(database: string, schema: string): Promise<void> {
    await this.execute(`create schema if not exists "${schema}"`, { includeQueryInError: true });
  }

  public async close() {
    await this.queryExecutor.close();
    if (this.options.sshTunnel) {
      await this.options.sshTunnel.close();
    }
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    return [];
  }

  public async persistStateMetadata() {
    // Unimplemented.
  }

  public async setMetadata(): Promise<void> {
    // Unimplemented.
  }

  private async hasSpectrumTables() {
    return (
      (
        await this.execute(
          `select 1
         from information_schema.tables
         where table_name = 'svv_external_tables'
           and table_schema = 'pg_catalog'`,
          { maxResults: 1, includeQueryInError: true }
        )
      ).rows.length > 0
    );
  }
}

class PgPoolExecutor {
  private pool: pg.Pool;
  constructor(clientConfig: pg.ClientConfig) {
    this.pool = new pg.Pool(clientConfig);
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
      onCancel?: OnCancel;
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    if (!options || !options.maxResults) {
      const result = await this.pool.query(statement);
      verifyUniqueColumnNames(result.fields);
      return result.rows;
    }
    const client = await this.pool.connect();
    client.on("error", err => {
      // tslint:disable-next-line: no-console
      console.error("pg.Client client error", err.message, err.stack);
    });
    try {
      // If we want to limit the returned results from redshift, we have two options:
      // (1) use cursors, or (2) use JDBC and configure a fetch size parameter. We use cursors
      // to avoid the need to run a JVM.
      // See https://docs.aws.amazon.com/redshift/latest/dg/declare.html for more details.
      const cursor: ICursor = client.query(new Cursor(statement));
      const result = await new Promise<any[]>((resolve, reject) => {
        options?.onCancel?.(() =>
          cursor.close(e => {
            if (e) {
              reject(e);
            }
          })
        );

        // It seems that when requesting one row back exactly, we run into some issues with
        // the cursor. I've filed a bug (https://github.com/brianc/node-pg-cursor/issues/55),
        // but setting a minimum of 2 resulting rows seems to do the trick.
        cursor.read(Math.max(2, options.maxResults), (err, rows, queryResult) => {
          try {
            verifyUniqueColumnNames(queryResult.fields);
          } catch (e) {
            reject(e);
          }
          if (err) {
            reject(err);
            return;
          }
          // Close the cursor after reading the first page of results.
          cursor.close(closeErr => {
            if (closeErr) {
              reject(closeErr);
            } else {
              // Limit results again, in case we had to increase the limit in the original request.
              resolve(rows.slice(0, options.maxResults));
            }
          });
        });
      });
      return result;
    } finally {
      client.release();
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
function convertFieldType(type: string) {
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
