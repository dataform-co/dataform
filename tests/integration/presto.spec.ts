import { expect } from "chai";
import * as http from "http";
import { Client } from "presto-client";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { sleep } from "df/common/promises";
import * as adapters from "df/core/adapters";
import { RedshiftAdapter } from "df/core/adapters/redshift";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, getTableRows, keyBy } from "df/tests/integration/utils";
import { PostgresFixture } from "df/tools/postgres/postgres_fixture";

const PRESTO_CONNECTION_DETAILS = {
  host: "127.0.0.1",
  port: 1234,
  headers: { "X-Presto-User": "df-integration-test" }
};

suite("@dataform/integration/presto", { parallel: true }, ({ before, after }) => {
  let dbadapter: dbadapters.IDbAdapter;

  before("create adapter", async () => {});

  test("run", { timeout: 60000 }, async () => {
    const client = new Client({ user: "testuser" });

    console.log("prestoRequest", prestoRequest);

    expect(true).to.equal(false);
  });
});

// There's no existing presto types library.
// There's an outstanding PR here for node-presto: https://github.com/tagomoris/presto-client-node/pull/29/files
// And a discussion about it here: https://github.com/tagomoris/presto-client-node/issues/28
interface PrestoRequest {
  nextUri: string;
}

async function prestoExecute(statement?: string) {
  let jobStatus = await prestoExecuteStatement(statement);
  console.log("jobStatus -> jobStatus", jobStatus);
  // If there's no nextURI then the job has completed successfully.
  if (jobStatus.nextUri as string) {
    await sleep(1000);
    jobStatus = await prestoCheckStatus(jobStatus.nextUri);
  }
}

async function prestoCheckStatus(nextURI?: string) {
  const path = nextURI.split(`:${PRESTO_CONNECTION_DETAILS.port}`)[1];
  return await prestoHttpRequest({ ...PRESTO_CONNECTION_DETAILS, method: "GET", path });
}

async function prestoExecuteStatement(statement?: string) {
  return await prestoHttpRequest(
    { ...PRESTO_CONNECTION_DETAILS, method: "POST", path: "/v1/statement" },
    statement
  );
}

// Node-presto doesn't use types and doesn't
function prestoHttpRequest(params: http.RequestOptions, postData?: any): Promise<PrestoRequest> {
  // HTTP docs available here:
  // https://github.com/prestodb/presto/wiki/HTTP-Protocol.
  return new Promise((resolve, reject) => {
    const req = http.request(params, res => {
      // Presto recommends waiting 50-100ms for 503 status code.
      if (res.statusCode !== 503 && res.statusCode !== 200) {
        return reject(new Error("statusCode=" + res.statusCode));
      }

      let body: any[] = [];
      res.on("data", chunk => {
        body.push(chunk);
      });

      res.on("end", () => {
        try {
          body = JSON.parse(Buffer.concat(body).toString());
        } catch (e) {
          reject(e);
        }
        resolve(body as any);
      });
    });

    req.on("error", err => {
      reject(err);
    });

    if (postData) {
      req.write(postData);
    }

    req.end();
  });
}
