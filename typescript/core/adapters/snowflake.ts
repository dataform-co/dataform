import * as protos from "@dataform/protos";
import { Adapter } from "./index";

export class SnowflakeAdapter implements Adapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    this.project = project;
  }

  queryableName(target: protos.ITarget) {
    return `\`${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  build(m: protos.IMaterialization, runConfig: protos.IRunConfig): protos.IExecutionTask[] {
    var statements: protos.IExecutionTask[] = [];
    statements.push({
      statement: `drop ${m.type == "view" ? "table" : "view"} if exists ${this.queryableName(m.target)}`,
      ignoreErrors: true
    });
    if (m.type == "incremental") {
      if (m.protected && runConfig.fullRefresh) {
        throw "Cannot run full-refresh on a protected table.";
      }
      if (!m.parsedColumns || m.parsedColumns.length == 0) {
        throw "Incremental models must have explicitly named column selects.";
      }
      statements.push({
        statement: `create ${runConfig.fullRefresh ? "or replace table" : "table if not exists"} ${this.queryableName(
          m.target
        )}
         ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
         as select * from (${m.query}) where false`
      });
      statements.push({
        statement: `
          insert ${this.queryableName(m.target)} (${m.parsedColumns.join(",")})
          select * from (
            ${m.query}
          ) ${runConfig.fullRefresh ? "" : `where ${m.where}`}`
      });
    } else {
      statements.push({
        statement: `
          create or replace ${m.type == "table" ? "table" : "view"} ${this.queryableName(m.target)}
          ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
          as select * from (
            ${m.query}
          )`
      });
    }
    return statements;
  }
}
