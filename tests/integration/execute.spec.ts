import { expect } from "chai";
import Long from "long";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { suite, test } from "df/testing";

suite("@dataform/integration/execute", { parallel: true }, () => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const dbadapter = new BigQueryDbAdapter(credentials);

  test("returned metadata includes jobReference and statistics", async () => {
    const query = `select 1 as test`;
    const { metadata } = await dbadapter.execute(query);
    const { bigquery: bqMetadata } = metadata;
    expect(bqMetadata).to.have.property("jobId");
    expect(bqMetadata.jobId).to.match(
      /^dataform-[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$/
    );
    expect(bqMetadata).to.have.property("totalBytesBilled");
    expect(bqMetadata.totalBytesBilled).to.eql(Long.fromNumber(0));
    expect(bqMetadata).to.have.property("totalBytesProcessed");
    expect(bqMetadata.totalBytesProcessed).to.eql(Long.fromNumber(0));
  });

  test("configured job prefix is added to table names", async () => {
    const query = `select 1 as test`;
    const { metadata } = await dbadapter.execute(query, { bigquery: { jobPrefix: "jobPrefix" } });
    const { bigquery: bqMetadata } = metadata;
    expect(bqMetadata).to.have.property("jobId");
    expect(bqMetadata.jobId).to.match(
      /^dataform-jobPrefix-[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$/
    );
  });

  suite("query limits work", { parallel: true }, () => {
    const query = `
      select 1 union all
      select 2 union all
      select 3 union all
      select 4 union all
      select 5`;

    for (const options of [
      { interactive: true, rowLimit: 2 },
      { interactive: false, rowLimit: 2 },
      { interactive: true, byteLimit: 30 },
      { interactive: false, byteLimit: 30 }
    ]) {
      test(`with options=${JSON.stringify(options)}`, async () => {
        const { rows } = await dbadapter.execute(query, options);
        expect(rows).to.eql([
          {
            f0_: 1
          },
          {
            f0_: 2
          }
        ]);
      });
    }
  });
});
