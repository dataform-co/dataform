import * as protos from "@dataform/protos";
import { BigQueryDbAdapter } from "./bigquery";
import { RedshiftDbAdapter } from "./redshift";
import { SnowflakeDbAdapter } from "./snowflake";
import { EventEmitter } from "events";

export type OnCancel = ((handleCancel: () => void) => void);

export interface DbAdapter {
  execute(statement: string, onCancel?: OnCancel): Promise<any[]>;
  evaluate(statement: string): Promise<void>;
  tables(): Promise<protos.ITarget[]>;
  table(target: protos.ITarget): Promise<protos.ITableMetadata>;
  prepareSchema(schema: string): Promise<void>;
}

export interface DbAdapterConstructor<T extends DbAdapter> {
  new (profile: protos.IProfile): T;
}

const registry: { [warehouseType: string]: DbAdapterConstructor<DbAdapter> } = {};

export function register(warehouseType: string, c: DbAdapterConstructor<DbAdapter>) {
  registry[warehouseType] = c;
}

export function create(profile: protos.IProfile, warehouseType?: string): DbAdapter {
  if (warehouseType) {
    if (!registry[warehouseType]) {
      throw Error(`Unsupported warehouse: ${warehouseType}`);
    }
    return new registry[warehouseType](profile);
  }
  if (!!profile.bigquery) {
    return new registry["bigquery"](profile);
  }
  if (!!profile.redshift) {
    return new registry["redshift"](profile);
  }
  if (!!profile.snowflake) {
    return new registry["snowflake"](profile);
  } else throw Error("Invalid profile.");
}

register("bigquery", BigQueryDbAdapter);
register("redshift", RedshiftDbAdapter);
register("snowflake", SnowflakeDbAdapter);
