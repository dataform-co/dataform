import * as protos from "@dataform/protos";
import { BigQueryAdapter } from "./bigquery";
import { RedshiftAdapter } from "./redshift";

export enum TableType {
  TABLE,
  VIEW
}

export interface Adapter {
  resolveTarget(target: protos.ITarget): string;
  createIfNotExists(target: protos.ITarget, query: string, type: TableType, partitionBy: string): string;
  createOrReplace(target: protos.ITarget, query: string, type: TableType, partitionBy: string): string;
  insertInto(target: protos.ITarget, columns: string[], query: string);
  dropIfExists(target: protos.ITarget, type: TableType): string;
  where(query: string, where: string): string;
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
