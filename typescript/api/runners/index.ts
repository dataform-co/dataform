import * as protos from "@dataform/protos";
import { BigQueryRunner } from "./bigquery";
import { RedshiftRunner } from "./redshift";
import { SnowflakeRunner } from "./snowflake";

export interface Runner {
  execute(statement: string): Promise<any[]>;
  tables(): Promise<protos.ITarget[]>;
  schema(target: protos.ITarget): Promise<protos.ITable>;
}

export interface RunnerConstructor<T extends Runner> {
  new (profile: protos.IProfile): T;
}

const registry: { [warehouseType: string]: RunnerConstructor<Runner> } = {};

export function register(warehouseType: string, c: RunnerConstructor<Runner>) {
  registry[warehouseType] = c;
}

export function create(profile: protos.IProfile, warehouseType?: string): Runner {
  if (warehouseType) {
    return new registry[warehouseType](profile);
  }
  if (!!profile.bigquery) {
    return new registry["bigquery"](profile);
  }
  if (!!profile.redshift) {
    return new registry["bigquery"](profile);
  }
  if (!!profile.snowflake) {
    return new registry["bigquery"](profile);
  }
  else throw Error("Invalid profile.");
}

register("bigquery", BigQueryRunner);
register("redshift", RedshiftRunner);
register("snowflake", SnowflakeRunner);
