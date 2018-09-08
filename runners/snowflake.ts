import { Runner } from "./index";
import * as protos from "../protos";
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
      } else {
        console.log("Successfully connected as id: " + this.connection.getId());
      }
    });
  }

  execute(statement: string) {
    return new Promise((resolve, reject) => {
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
}

import * as testcreds from "../testcreds";

const runner = new SnowflakeRunner({ snowflake: testcreds.snowflake });

runner.execute("select 1 as test").then(rows => console.log(rows));
