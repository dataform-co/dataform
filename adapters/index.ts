import * as protos from "../protos";

export interface Adapter {
  queryableName: (target: protos.ITarget) => string;
  materializeStatements: (materialization: protos.Materialization, runConfig: protos.IRunConfig) => string[];
}

export class GenericAdapter implements Adapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    this.project = project;
  }

  queryableName(target: protos.ITarget) {
    return `"${target.schema || this.project.defaultSchema}"."${target.name}"`;
  }

  materializeStatements(m: protos.IMaterialization, runConfig: protos.IRunConfig) {
    var statements: string[] = [];
    if (m.type == "incremental") {
      if (m.protected && runConfig.fullRefresh) {
        throw "Cannot run full-refresh on a protected table.";
      }
      // Drop the table if it exists and we are doing a full refresh.
      if (runConfig.fullRefresh) {
        statements.push(`drop table if exists ${this.queryableName(m.target)}`);
      }
      statements.push(
        `create table if not exists ${this.queryableName(m.target)}
         ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
         as select * from (${m.query}) where false`
      );
      statements.push(`insert ${this.queryableName(m.target)} (v1, v2, v3) select * from (${m.query})`);
    }
    if (m.type == "table" || m.type == "view") {
      statements.push(`drop view if exists ${this.queryableName(m.target)}`);
      statements.push(`drop table if exists ${this.queryableName(m.target)}`);
      statements.push(
        `create ${m.type == "table" ? "table" : "view"} ${this.queryableName(m.target)}
         ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
         as select * from (${m.query})`
      )
    }
    return statements;
  }
}
