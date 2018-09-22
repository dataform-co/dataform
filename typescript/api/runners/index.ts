import * as protos from "@dataform/protos";
import { BigQueryRunner } from "./bigquery";
import { RedshiftRunner } from "./redshift";
import { SnowflakeRunner } from "./snowflake";

export interface Runner {
  execute(statement: string): Promise<any[]>;
}

export interface RunnerConstructor<T extends Runner> {
  new (profile: protos.IProfile): T;
}

const registry: { [warehouseType: string]: RunnerConstructor<Runner> } = {};

export function register(warehouseType: string, c: RunnerConstructor<Runner>) {
  registry[warehouseType] = c;
}

export function create(warehouseType: string, profile: protos.IProfile): Runner {
  return new registry[warehouseType](profile);
}

register("bigquery", BigQueryRunner);
register("redshift", RedshiftRunner);
register("snowflake", SnowflakeRunner);
