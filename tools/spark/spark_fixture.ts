// Let's not build spark from source; requires maven and scala.
// https://spark.apache.org/docs/latest/building-spark.html
import { execSync } from "child_process";
import * as dbadapters from "df/api/dbadapters";
import { sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";

const USE_CLOUD_BUILD_NETWORK = !!process.env.USE_CLOUD_BUILD_NETWORK;
const DOCKER_CONTAINER_NAME = "spark-df-integration-testing";

export class SparkFixture {
  public static readonly host = USE_CLOUD_BUILD_NETWORK ? DOCKER_CONTAINER_NAME : "localhost";

  private static imageLoaded = false;

  constructor(port: number, setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting spark", async () => {
      // Run the spark Docker image.
      execSync("docker-compose up -d");

      const dbadapter = await dbadapters.create(
        {
          // These values are configured in both the docker-compose file and hive-site.xml.
          databaseName: "metastore",
          username: "user",
          password: "password",
          port,
          host: SparkFixture.host
        },
        "postgres",
        { disableSslForTestsOnly: true }
      );

      // Block until spark is ready to accept requests.
      await sleepUntil(async () => {
        try {
          await dbadapter.execute("select 1");
          return true;
        } catch (e) {
          return false;
        }
      });
    });

    tearDown("stopping spark", () => {
      execSync(`docker-compose down`);
    });
  }
}
