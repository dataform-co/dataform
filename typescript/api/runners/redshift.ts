import { Runner } from "./index";
import * as protos from "@dataform/protos";
const Redshift = require("node-redshift");

export class RedshiftRunner implements Runner {
  private profile: protos.IProfile;
  private client: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = new Redshift(profile.redshift);
  }

  execute(statement: string) {
    return this.client.query(statement).then(result => result.rows);
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

  schema(target: protos.ITarget): Promise<protos.ITable> {
    return this.execute(
      `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' AND table_name = '${
        target.name
      }'`
    ).then(rows => ({
      target: target,
      schema: {
        fields: rows.map(row => ({
          name: row.column_name,
          primitive: row.data_type,
          flags: row.is_nullable && row.is_nullable == "YES" ? ["nullable"] : []
        }))
      }
    }));
  }

  prepareSchema(schema: string): Promise<void> {
    return this.execute(`create schema if not exists "${schema}"`);
  }
}
