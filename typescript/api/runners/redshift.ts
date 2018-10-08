import { Runner } from "./index";
import * as protos from "@dataform/protos";
const Redshift = require("node-redshift");

export class RedshiftRunner implements Runner {
  private profile: protos.IProfile;
  private client: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = new Redshift(profile.redshift);
  }

  execute(statement: string) {
    return this.client.query(statement).then(result => result.rows);
  }

  tables(): Promise<protos.ITarget[]> {
    throw Error("Unimplemented");
  }

  schema(target: protos.ITarget): Promise<protos.ITable> {
    throw Error("Unimplemented");
  }
}
