import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter, OnCancel } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import { ConnectionPool } from "mssql";

const INFORMATION_SCHEMA_SCHEMA_NAME = "information_schema";
const TABLE_NAME_COL_NAME = "table_name";
const TABLE_SCHEMA_COL_NAME = "table_schema";
const TABLE_TYPE_COL_NAME = "table_type";
const COLUMN_NAME_COL_NAME = "column_name";
const DATA_TYPE_COL_NAME = "data_type";
const IS_NULLABLE_COL_NAME = "is_nullable";

export class SQLDataWarehouseDBAdapter implements IDbAdapter {
  private pool: Promise<ConnectionPool>;

  constructor(credentials: Credentials) {
    const sqlDataWarehouseCredentials = credentials as dataform.ISQLDataWarehouse;
    this.pool = new Promise((resolve, reject) => {
      const conn = new ConnectionPool({
        server: sqlDataWarehouseCredentials.server,
        port: sqlDataWarehouseCredentials.port,
        user: sqlDataWarehouseCredentials.username,
        password: sqlDataWarehouseCredentials.password,
        database: sqlDataWarehouseCredentials.database,
        options: {
          encrypt: true
        }
      }).connect();
      conn
        .then(pool => {
          pool.on("error", err => {
            throw new Error(err);
          });
          resolve(conn);
        })
        .catch(e => reject(e));
    });
  }

  public async execute(
    statement: string,
    options: {
      onCancel?: OnCancel;
      maxResults?: number;
    } = { maxResults: 1000 }
  ): Promise<any[]> {
    const request = (await this.pool).request();
    if (options && options.onCancel) {
      options.onCancel(() => request.cancel());
    }

    return await new Promise<any[]>((resolve, reject) => {
      request.stream = true;

      const rows: any[] = [];

      request
        .on("row", row => {
          if (options && options.maxResults && rows.length >= options.maxResults) {
            request.cancel();
            resolve(rows);
            return;
          }
          rows.push(row);
        })
        .on("error", err => reject(err))
        .on("done", () => resolve(rows));

      // tslint:disable-next-line: no-floating-promises
      request.query(statement);
    });
  }

  public async evaluate(statement: string) {
    await this.execute(`explain ${statement}`);
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const result = await this.execute(
      `select ${TABLE_SCHEMA_COL_NAME}, ${TABLE_NAME_COL_NAME} from ${INFORMATION_SCHEMA_SCHEMA_NAME}.tables`,
      { maxResults: 10000 }
    );
    return result.map(row => ({
      schema: row[TABLE_SCHEMA_COL_NAME],
      name: row[TABLE_NAME_COL_NAME]
    }));
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const [tableData, columnData] = await Promise.all([
      this.execute(
        `select ${TABLE_TYPE_COL_NAME} from ${INFORMATION_SCHEMA_SCHEMA_NAME}.tables 
          where ${TABLE_SCHEMA_COL_NAME} = '${target.schema}' AND ${TABLE_NAME_COL_NAME} = '${target.name}'`
      ),
      this.execute(
        `select ${COLUMN_NAME_COL_NAME}, ${DATA_TYPE_COL_NAME}, ${IS_NULLABLE_COL_NAME}
       from ${INFORMATION_SCHEMA_SCHEMA_NAME}.columns
       where ${TABLE_SCHEMA_COL_NAME} = '${target.schema}' AND ${TABLE_NAME_COL_NAME} = '${target.name}'`
      )
    ]);

    if (tableData.length === 0) {
      throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
    }

    // The table exists.
    return {
      target,
      type: tableData[0][TABLE_TYPE_COL_NAME] === "VIEW" ? "view" : "table",
      fields: columnData.map(row => ({
        name: row[COLUMN_NAME_COL_NAME],
        primitive: row[DATA_TYPE_COL_NAME],
        flags: row[IS_NULLABLE_COL_NAME] && row[IS_NULLABLE_COL_NAME] === "YES" ? ["nullable"] : []
      }))
    };
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    return this.execute(`SELECT TOP ${limitRows} * FROM "${target.schema}"."${target.name}"`);
  }

  public async prepareSchema(schema: string): Promise<void> {
    await this.execute(
      `if not exists ( select schema_name from ${INFORMATION_SCHEMA_SCHEMA_NAME}.schemata where schema_name = '${schema}' ) 
            begin
              exec sp_executesql N'create schema ${schema}'
            end `
    );
  }

  public async close() {
    await (await this.pool).close();
  }
}
