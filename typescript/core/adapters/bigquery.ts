import * as protos from "@dataform/protos";
import { Adapter, TableType } from "./index";

export class BigQueryAdapter implements Adapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    this.project = project;
  }

  resolveTarget(target: protos.ITarget) {
    return `\`${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  createIfNotExists(
    target: protos.ITarget,
    query: string,
    type: TableType,
    partitionBy?: string
  ) {
    return `
      create ${
        type == TableType.TABLE ? "table" : "view"
      } if not exists ${this.resolveTarget(target)}
      ${partitionBy ? `partition by ${partitionBy}` : ""}
      as ${query}`;
  }
  createOrReplace(
    target: protos.ITarget,
    query: string,
    type: TableType,
    partitionBy?: string
  ) {
    return `
      create or replace ${
        type == TableType.TABLE ? "table" : "view"
      } ${this.resolveTarget(target)}
      ${partitionBy ? `partition by ${partitionBy}` : ""}
      as ${query}`;
  }
  insertInto(target: protos.ITarget, columns: string[], query: string) {
    return `
      insert ${this.resolveTarget(target)} (${columns.join(",")})
      ${query}`;
  }

  dropIfExists(target: protos.ITarget, type: TableType) {
    return `drop ${
      type == TableType.TABLE ? "table" : "view"
    } if exists ${this.resolveTarget(target)}`;
  }

  where(query: string, where: string) {
    return `select * from (
        ${query})
        where ${where}`;
  }
}
