import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import * as pg from "pg";
import * as Cursor from "pg-cursor";

interface ICursor {
  read: (rowCount: number, callback: (err: Error, rows: any[]) => void) => void;
  close: (callback: (err: Error) => void) => void;
}

export class RedshiftDbAdapter implements IDbAdapter {
  private pool: pg.Pool;

  constructor(credentials: Credentials) {
    const redshiftCredentials = credentials as dataform.IJDBC;
    this.pool = new pg.Pool({
      host: redshiftCredentials.host,
      port: redshiftCredentials.port,
      user: redshiftCredentials.username,
      password: redshiftCredentials.password,
      database: redshiftCredentials.databaseName,
      ssl: true
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
    try {
      // If we want to limit the returned results from redshift, we have two options:
      // (1) use cursors, or (2) use JDBC and configure a fetch size parameter. We use cursors
      // to avoid the need to run a JVM.
      // See https://docs.aws.amazon.com/redshift/latest/dg/declare.html for more details.
      const cursor: ICursor = client.query(new Cursor(statement));
      const result = await new Promise<any[]>((resolve, reject) => {
        cursor.read(options.maxResults, (err, rows) => {
          if (err) {
            reject(err);
            return;
          }
          // Close the cursor after reading the first page of results.
          cursor.close(closeErr => {
            if (closeErr) {
              reject(closeErr);
            } else {
              resolve(rows);
            }
          });
        });
      });
      return result;
    } finally {
      client.release();
    }
  }

  public async evaluate(statement: string) {
    await this.execute(`explain ${statement}`);
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const rows = await this.execute(
      `select table_name, table_schema
     from information_schema.tables
     where table_schema != 'information_schema'
       and table_schema != 'pg_catalog'
       and table_schema != 'pg_internal'
       union select tablename as table_name, schemaname as table_schema from svv_external_tables`
    );
    return rows.map(row => ({
      schema: row.table_schema,
      name: row.table_name
    }));
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const [columnResults, tableResults, externalTableResults] = await Promise.all([
      this.execute(
        `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' and table_name = '${target.name}'
       union
       select columnname as column_name, external_type as data_type, 'not_available' as is_nullable 
       from svv_external_columns 
       where schemaname = '${target.schema}' and tablename = '${target.name}'`
      ),
      this.execute(
        `select table_type from information_schema.tables where table_schema = '${target.schema}' and table_name = '${target.name}'`
      ),
      this.execute(
        `select 'TABLE' as table_type from svv_external_tables where schemaname = '${target.schema}' and tablename = '${target.name}'`
      )
    ]);
    const allTableResults = tableResults.concat(externalTableResults);
    if (allTableResults.length > 0) {
      // The table exists.
      return {
        target,
        type: allTableResults[0].table_type === "VIEW" ? "view" : "table",
        fields: columnResults.map(row => ({
          name: row.column_name,
          primitive: row.data_type,
          flags: row.is_nullable && row.is_nullable === "YES" ? ["nullable"] : []
        }))
      };
    } else {
      throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
    }
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    return this.execute(`SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`);
  }

  public async prepareSchema(schema: string): Promise<void> {
    await this.execute(`create schema if not exists "${schema}"`);
  }
}
