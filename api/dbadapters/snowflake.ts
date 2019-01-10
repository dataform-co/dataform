import { DbAdapter } from "./index";
import * as protos from "@dataform/protos";

const Snowflake = require("snowflake-sdk");

export class SnowflakeDbAdapter implements DbAdapter {
  private connection: any;

  constructor(profile: protos.IProfile) {
    this.connection = Snowflake.createConnection(profile.snowflake);
    this.connection.connect((err, conn) => {
      if (err) {
        console.error("Unable to connect: " + err.message);
      }
    });
  }

  execute(statement: string) {
    return new Promise<any[]>((resolve, reject) => {
      this.connection.execute({
        sqlText: statement,
        complete: function(err, _, rows) {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }

  evaluate(statement: string): Promise<void> {
    return this.connection.query(`explain ${statement}`).then(() => {});
  }

  tables(): Promise<protos.ITarget[]> {
    return this.execute(
      `select table_name, table_schema
         from information_schema.tables
         where table_schema != 'information_schema'
           and table_schema != 'pg_catalog'
           and table_schema != 'pg_internal'`
    ).then(rows =>
      rows.map(row => ({
        schema: row.table_schema,
        name: row.table_name
      }))
    );
  }

  table(target: protos.ITarget): Promise<protos.ITable> {
    return Promise.all([
      this.execute(
        `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      ),
      this.execute(
        `select table_type from information_schema.tables where table_schema = '${target.schema}' AND table_name = '${
          target.name
          }'`
      )
    ]).then(results => ({
      target: target,
      type: results[1][0] ? (results[1][0].TABLE_TYPE == "VIEW" ? "view" : "table") : null,
      fields: results[0].map(row => ({
        name: row.COLUMN_NAME,
        primitive: row.DATA_TYPE,
        flags: row.IS_NULLABLE && row.IS_NULLABLE == "YES" ? ["nullable"] : []
      }))
    }));
  }

  prepareSchema(schema: string): Promise<void> {
    return this.execute(`create schema if not exists "${schema}"`).then(() => {});
  }
}
