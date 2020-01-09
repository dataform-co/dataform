import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import * as pg from "pg";
import * as Cursor from "pg-cursor";
import * as PromisePool from "promise-pool-executor";

interface ICursor {
  read: (rowCount: number, callback: (err: Error, rows: any[]) => void) => void;
  close: (callback: (err: Error) => void) => void;
}

export class RedshiftDbAdapter implements IDbAdapter {
  private queryExecutor: IPgQueryExecutor;

  constructor(
    credentials: Credentials,
    // Intended for temporary testing, not included in a permanent API.
    usePgPool: boolean
  ) {
    this.queryExecutor = usePgPool
      ? new PgPoolExecutor(credentials as dataform.IJDBC)
      : new PgClientExecutor(credentials as dataform.IJDBC);
  }

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    const rows = await this.queryExecutor.execute(statement, options);
    return { rows };
  }

  public async evaluate(statement: string) {
    await this.execute(`explain ${statement}`);
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
      { maxResults: 10000 }
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
       }`
      ),
      this.execute(
        `select table_type from information_schema.tables where table_schema = '${target.schema}' and table_name = '${target.name}'`
      ),
      hasSpectrumTables
        ? this.execute(
            `select 'TABLE' as table_type from svv_external_tables where schemaname = '${target.schema}' and tablename = '${target.name}'`
          )
        : { rows: [] }
    ]);
    const allTableResults = tableResults.rows.concat(externalTableResults.rows);
    if (allTableResults.length > 0) {
      // The table exists.
      return {
        target,
        type: allTableResults[0].table_type === "VIEW" ? "view" : "table",
        fields: columnResults.rows.map(row => ({
          name: row.column_name,
          primitive: row.data_type,
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

  public async prepareSchema(schema: string): Promise<void> {
    await this.execute(`create schema if not exists "${schema}"`);
  }

  public async close() {
    await this.queryExecutor.close();
  }

  private async hasSpectrumTables() {
    return (
      (await this.execute(
        `select 1
         from information_schema.tables
         where table_name = 'svv_external_tables'
           and table_schema = 'pg_catalog'`,
        { maxResults: 1 }
      )).rows.length > 0
    );
  }
}

interface IPgQueryExecutor {
  execute(
    statement: string,
    options: {
      maxResults?: number;
    }
  ): Promise<any[]>;
  close(): Promise<void>;
}

class PgClientExecutor implements IPgQueryExecutor {
  private credentials: dataform.IJDBC;
  private pool: PromisePool.PromisePoolExecutor;

  constructor(credentials: dataform.IJDBC) {
    this.credentials = credentials;
    // Limit DB client concurrency.
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: 10,
      frequencyWindow: 1000,
      frequencyLimit: 10
    });
  }

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    return this.pool
      .addSingleTask({
        generator: () => this.executeInsidePool(statement, options)
      })
      .promise();
  }

  public async close() {}

  private async executeInsidePool(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    const client = new pg.Client({
      host: this.credentials.host,
      port: this.credentials.port,
      user: this.credentials.username,
      password: this.credentials.password,
      database: this.credentials.databaseName,
      ssl: true
    });
    client.on("error", err => {
      console.error("pg.Client client error", err.message, err.stack);
    });
    await client.connect();
    try {
      if (!options || !options.maxResults) {
        const result = await client.query(statement);
        return result.rows;
      }
      // If we want to limit the returned results from redshift, we have two options:
      // (1) use cursors, or (2) use JDBC and configure a fetch size parameter. We use cursors
      // to avoid the need to run a JVM.
      // See https://docs.aws.amazon.com/redshift/latest/dg/declare.html for more details.
      const cursor: ICursor = client.query(new Cursor(statement));
      return await new Promise<any[]>((resolve, reject) => {
        // It seems that when requesting one row back exactly, we run into some issues with
        // the cursor. I've filed a bug (https://github.com/brianc/node-pg-cursor/issues/55),
        // but setting a minimum of 2 resulting rows seems to do the trick.
        cursor.read(Math.max(2, options.maxResults), (err, rows) => {
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
    } finally {
      await client.end();
    }
  }
}

class PgPoolExecutor implements IPgQueryExecutor {
  private pool: pg.Pool;
  constructor(credentials: dataform.IJDBC) {
    this.pool = new pg.Pool({
      host: credentials.host,
      port: credentials.port,
      user: credentials.username,
      password: credentials.password,
      database: credentials.databaseName,
      ssl: true
    });
    // https://node-postgres.com/api/pool#events
    // Idle clients in the pool are still connected to the remote host and as such can
    // emit errors. If/when they do, they will automatically be removed from the pool,
    // but we still need to handle the error to prevent crashing the process.
    this.pool.on("error", err => {
      console.error("pg.Pool idle client error", err.message, err.stack);
    });
  }

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    if (!options || !options.maxResults) {
      const result = await this.pool.query(statement);
      return result.rows;
    }
    const client = await this.pool.connect();
    client.on("error", err => {
      console.error("pg.Client client error", err.message, err.stack);
    });
    try {
      // If we want to limit the returned results from redshift, we have two options:
      // (1) use cursors, or (2) use JDBC and configure a fetch size parameter. We use cursors
      // to avoid the need to run a JVM.
      // See https://docs.aws.amazon.com/redshift/latest/dg/declare.html for more details.
      const cursor: ICursor = client.query(new Cursor(statement));
      const result = await new Promise<any[]>((resolve, reject) => {
        // It seems that when requesting one row back exactly, we run into some issues with
        // the cursor. I've filed a bug (https://github.com/brianc/node-pg-cursor/issues/55),
        // but setting a minimum of 2 resulting rows seems to do the trick.
        cursor.read(Math.max(2, options.maxResults), (err, rows) => {
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
