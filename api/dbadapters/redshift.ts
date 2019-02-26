import { DbAdapter } from "./index";
import * as protos from "@dataform/protos";
import * as Promise from "bluebird";

const Redshift: RedshiftType = require("node-redshift");

type RedshiftType = {
  new (client: {
    host?: string;
    port?: number | Long;
    user?: string;
    password?: string;
    database?: string;
    ssl: boolean;
  });
  query: (query: string) => Promise<{ rows: any[] }>;
};

export class RedshiftDbAdapter implements DbAdapter {
  private client: RedshiftType;

  constructor(profile: protos.IProfile) {
    this.client = new Redshift(Object.assign({ ssl: true }, profile.redshift));
  }

  execute(statement: string) {
    return this.client.query(statement).then(result => result.rows);
  }

  evaluate(statement: string) {
    return this.client.query(`explain ${statement}`).then(() => {});
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

  table(target: protos.ITarget): Promise<protos.ITableMetadata> {
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
    ]).then(results => {
      if (results[1].length > 0) {
        // The table exists.
        return {
          target: target,
          type: results[1][0].table_type == "VIEW" ? "view" : "table",
          fields: results[0].map(row => ({
            name: row.column_name,
            primitive: row.data_type,
            flags: row.is_nullable && row.is_nullable == "YES" ? ["nullable"] : []
          }))
        };
      } else throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
    });
  }

  prepareSchema(schema: string): Promise<void> {
    return this.execute(`create schema if not exists "${schema}"`).then(() => {});
  }
}
