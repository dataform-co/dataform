import * as protos from "@dataform/protos";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";
import { SnowflakeAdapter } from "./snowflake";

export interface Adapter {
  queryableName: (target: protos.ITarget) => string;
  build: (materialization: protos.IMaterialization, runConfig: protos.IRunConfig) => protos.IExecutionTask[];
}

export interface AdapterConstructor<T extends Adapter> {
  new (projectConfig: protos.IProjectConfig): T;
}

const registry: { [warehouseType: string]: AdapterConstructor<Adapter> } = {};

export function register(warehouseType: string, c: AdapterConstructor<Adapter>) {
  registry[warehouseType] = c;
}

export function create(projectConfig: protos.IProjectConfig): Adapter {
  return new registry[projectConfig.warehouse](projectConfig);
}

register("bigquery", BigQueryAdapter);
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
