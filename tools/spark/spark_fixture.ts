// Let's not build spark from source; requires maven and scala.
// https://spark.apache.org/docs/latest/building-spark.html
import { execSync } from "child_process";
import * as dbadapters from "df/api/dbadapters";
import { sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";

const USE_CLOUD_BUILD_NETWORK = !!process.env.USE_CLOUD_BUILD_NETWORK;
const DOCKER_CONTAINER_NAME = "spark-df-integration-testing";
// This is the serving port of the master node.
const SPARK_SERVE_PORT = 8080;

export class SparkFixture {
  public static readonly host = USE_CLOUD_BUILD_NETWORK ? DOCKER_CONTAINER_NAME : "localhost";

  private static imageLoaded = false;

  constructor(port: number, setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting spark", async () => {
      if (!SparkFixture.imageLoaded) {
        // Load the spark image into the local Docker daemon.
        execSync("tools/spark/spark_image.executable");
        SparkFixture.imageLoaded = true;
      }
      // Run the spark Docker image.
      // docker run --name spark -p 8080:8080 --hostname localhost bitnami/spark:latest
      execSync(
        [
          "docker run",
          "--rm",
          `--name ${DOCKER_CONTAINER_NAME}`,
          "-d",
          `-p ${port}:${SPARK_SERVE_PORT}`,
          USE_CLOUD_BUILD_NETWORK ? "--network cloudbuild" : "",
          "--hostname localhost",
          "bazel/tools/spark:spark_image"
        ].join(" ")
      );

      // TODO: implement adapter.

      // Block until spark is ready to accept requests.
      await sleepUntil(async () => {
        try {
          // await dbadapter.execute("select 1");
          return true;
        } catch (e) {
          return false;
        }
      });
    });

    tearDown("stopping spark", () => {
      execSync(`docker stop ${DOCKER_CONTAINER_NAME}`);
    });
  }
}
