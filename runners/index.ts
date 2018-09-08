import * as protos from "../protos";
import { BigQueryRunner } from "./bigquery";
import { RedshiftRunner } from "./redshift";
import { SnowflakeRunner } from "./snowflake";

export interface Runner {
  execute(statement: string): Promise<any>;
}

export interface RunnerConstructor<T extends Runner> {
  new(profile: protos.IProfile): T;
}

function create<T extends Runner>(c: RunnerConstructor<T>, profile: protos.IProfile) {
  return new c(profile);
}
