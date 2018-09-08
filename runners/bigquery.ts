import { Runner } from "./index";
import * as protos from "../protos";
const BigQuery = require("@google-cloud/bigquery");

export class BigQueryRunner implements Runner {
  private profile: protos.IProfile;
  private client: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = BigQuery(profile.bigquery.projectId, {
      credentials: profile.bigquery.keyFile
    });
  }

  execute(statement: string) {
    return this.client
      .query({
        useLegacySql: false,
        query: statement
      });
  }
}


import * as testcreds from "../testcreds";

const runner = new BigQueryRunner({
  bigquery: {
    projectId: "tada-analytics",
    keyFile: testcreds.bigquery
  }
});

runner.execute("select 1 as test").then(rows => console.log(rows));
