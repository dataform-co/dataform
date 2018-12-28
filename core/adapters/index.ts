import * as protos from "@dataform/protos";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";
import { SnowflakeAdapter } from "./snowflake";
import { Tasks } from "../tasks";


export interface Adapter {
  resolveTarget(target: protos.ITarget): string;

  materializeTasks(materialization: protos.IMaterialization, runConfig: protos.IRunConfig, table: protos.ITable): Tasks;
  assertTasks(materialization: protos.IAssertion, projectConfig: protos.IProjectConfig): Tasks;
}

export interface AdapterConstructor<T extends Adapter> {
  new (projectConfig: protos.IProjectConfig): T;
}

const registry: { [warehouseType: string]: AdapterConstructor<Adapter> } = {};

export function register(warehouseType: string, c: AdapterConstructor<Adapter>) {
  registry[warehouseType] = c;
}

export function create(projectConfig: protos.IProjectConfig): Adapter {
  if (!registry[projectConfig.warehouse]) {
    throw Error(`Unsupported warehouse: ${projectConfig.warehouse}`);
  }
  return new registry[projectConfig.warehouse](projectConfig);
}

register("bigquery", BigQueryAdapter);
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
