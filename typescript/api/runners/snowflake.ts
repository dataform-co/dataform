import { Runner } from "./index";
import * as protos from "@dataform/protos";
const Snowflake = require("snowflake-sdk");

export class SnowflakeRunner implements Runner {
  private profile: protos.IProfile;
  private connection: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.connection = Snowflake.createConnection(profile.snowflake);
    this.connection.connect((err, conn) => {
      if (err) {
        console.error("Unable to connect: " + err.message);
      }
    });
  }

  execute(statement: string) {
    return new Promise<any[]>((resolve, reject) => {
      this.connection.execute({
        sqlText: statement,
        complete: function(err, _, rows) {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }

  tables(): Promise<protos.ITarget[]> {
    throw Error("Unimplemented");
  }

  schema(target: protos.ITarget): Promise<protos.ISchema> {
    throw Error("Unimplemented");
  }
}
