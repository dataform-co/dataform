import * as protos from "@dataform/protos";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";
import { SnowflakeAdapter } from "./snowflake";
import { Tasks } from "../tasks";

export interface IAdapter {
  resolveTarget(target: protos.ITarget): string;

  materializeTasks(materialization: protos.IMaterialization, runConfig: protos.IRunConfig, table: protos.ITable): Tasks;
  assertTasks(materialization: protos.IAssertion, projectConfig: protos.IProjectConfig): Tasks;
}

export interface AdapterConstructor<T extends IAdapter> {
  new (projectConfig: protos.IProjectConfig): T;
}

const registry: { [warehouseType: string]: AdapterConstructor<IAdapter> } = {};

export function register(warehouseType: string, c: AdapterConstructor<IAdapter>) {
  registry[warehouseType] = c;
}

export function create(projectConfig: protos.IProjectConfig): IAdapter {
  if (!registry[projectConfig.warehouse]) {
    throw Error(`Unsupported warehouse: ${projectConfig.warehouse}`);
  }
  return new registry[projectConfig.warehouse](projectConfig);
}

register("bigquery", BigQueryAdapter);
register("redshift", RedshiftAdapter);
register("snowflake", SnowflakeAdapter);
