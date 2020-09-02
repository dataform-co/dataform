import { execSync } from "child_process";
import * as dbadapters from "df/api/dbadapters";
import { sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";

const DOCKER_CONTAINER_NAME = "postgres-df-integration-testing";

const useCloudBuildNetwork = process.env.USE_CLOUD_BUILD_NETWORK;

export class PostgresFixture {
  public static readonly host = useCloudBuildNetwork ? DOCKER_CONTAINER_NAME : "localhost";

  private static imageLoaded = false;

  constructor(port: number, setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting postgres", async () => {
      if (!PostgresFixture.imageLoaded) {
        // Load the postgres image to the local Docker daemon.
        execSync("tools/postgres/postgres_image.executable");
        PostgresFixture.imageLoaded = true;
      }
      console.log("BEN BEN BEN loaded");
      execSync(
        [
          "docker run",
          "--rm",
          `--name ${DOCKER_CONTAINER_NAME}`,
          "-e POSTGRES_PASSWORD=password",
          "-d",
          `-p ${port}:5432`,
          useCloudBuildNetwork ? "--network cloudbuild" : "",
          "bazel/tools/postgres:postgres_image"
        ].join(" ")
      );
      console.log("BEN BEN BEN started");
      const dbadapter = await dbadapters.create(
        {
          username: "postgres",
          databaseName: "postgres",
          password: "password",
          port: 5432,
          host: PostgresFixture.host
        },
        "postgres",
        { disableSslForTestsOnly: true }
      );
      console.log("BEN BEN BEN db executor created");
      await sleepUntil(async () => {
        try {
          await dbadapter.execute("select 1");
          return true;
        } catch (e) {
          console.log("BEN BEN BEN error", String(e));
          return false;
        }
      });
      console.log("BEN BEN BEN ready");
    });

    tearDown("stopping postgres", () => {
      execSync(`docker stop postgres-df-integration-testing`);
    });
  }
}
