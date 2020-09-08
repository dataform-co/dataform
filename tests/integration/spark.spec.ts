import { expect } from "chai";

import * as dbadapters from "df/api/dbadapters";
import { suite, test } from "df/testing";
import { PostgresFixture } from "df/tools/postgres/postgres_fixture";
import { SparkFixture } from "df/tools/spark/spark_fixture";

suite("@dataform/integration/spark", { parallel: true }, ({ before, after }) => {
  let dbadapter: dbadapters.IDbAdapter;

  const spark = new SparkFixture(10000, before, after);

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(
      {
        databaseName: "metastore",
        username: "user",
        password: "password",
        port: 10000,
        host: PostgresFixture.host
      },
      "postgres",
      { disableSslForTestsOnly: true }
    );
  });

  test("run", { timeout: 60000 }, async () => {
    await dbadapter.execute(`SELECT 1`);
  });
});
