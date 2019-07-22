import { Credentials } from "@dataform/api/commands/credentials";
import { DbAdapter, OnCancel } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";

const sql = require("mssql");

interface IRequest {
  query: (query: string) => Promise<{ rows: any[] }>;
}

interface ISQLDataWarehouseConfig {
  server?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  encrypt: boolean;
}

export class SQLDataWarehouseDBAdapter implements DbAdapter {
  private pool: any;

  constructor(credentials: Credentials) {
    const sqlDataWarehouseCredentials = credentials as dataform.ISQLDataWarehouse;
    const config: ISQLDataWarehouseConfig = {
      server: sqlDataWarehouseCredentials.server,
      port: sqlDataWarehouseCredentials.port,
      user: sqlDataWarehouseCredentials.username,
      password: sqlDataWarehouseCredentials.password,
      database: sqlDataWarehouseCredentials.databaseName,
      encrypt: true
    };
    const conn = new sql.ConnectionPool(config).connect();

    conn.then(pool =>{
      pool.on('error',err =>{
        throw new Error(err);
      })
    })

    this.pool = conn;
  }

  public async execute(statement: string, onCancel?: OnCancel) {
    let pool = await this.pool;
    let request = pool.request();

    if(onCancel){
      onCancel(()=>{
        request.cancel();
      })
    }

    return request
          .query(statement)
          .then(result => result.recordset);
  }

  public async evaluate(statement: string) {
    let pool = await this.pool;
    return pool.request()
          .query(`explain ${statement}`)
          .then(() => {});
  }

  public async tables(): Promise<dataform.ITarget[]> {
    let result = await this.execute(`select table_name, table_schema from information_schema.tables`)
    return result.map(row => ({
      schema: row.table_schema,
      name: row.table_name
    }))
  }

  public table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    return Promise.all([
      this.execute(
        `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      ),
      this.execute(
        `select table_type from information_schema.tables 
          where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      )
    ]).then(results => {
      if (results[1].length > 0) {
        // The table exists.
        return {
          target,
          type: results[1][0].table_type == "VIEW" ? "view" : "table",
          fields: results[0].map(row => ({
            name: row.column_name,
            primitive: row.data_type,
            flags: row.is_nullable && row.is_nullable == "YES" ? ["nullable"] : []
          }))
        };
      } else {
        throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
      }
    });
  }

  public async prepareSchema(schema: string): Promise<void> {
    return Promise.resolve().then(() =>
      this.execute(`if not exists ( select schema_name from information_schema.schemata where schema_name = '${schema}' ) 
            begin
              exec sp_executesql N'create schema ${schema}'
            end `).then(() => {})
    );
  }
}
