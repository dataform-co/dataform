import * as path from "path";

import { exec, execSync, spawn } from "child_process";
import * as dbadapters from "df/api/dbadapters";
import { sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";

const USE_CLOUD_BUILD_NETWORK = !!process.env.USE_CLOUD_BUILD_NETWORK;
const DOCKER_CONTAINER_NAME = "presto-df-integration-testing";

export class PrestoFixture {
  public static readonly PRESTO_TEST_CREDENTIALS = {
    host: USE_CLOUD_BUILD_NETWORK ? DOCKER_CONTAINER_NAME : "localhost",
    port: 2345,
    user: "df-integration-tests",
    catalog: "memory",
    schema: "default"
  };

  private static imageLoaded = false;

  constructor(setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting presto", async () => {
      if (!PrestoFixture.imageLoaded) {
        // Load the presto image into the local Docker daemon.
        execSync("tools/presto/presto_image.executable");
        PrestoFixture.imageLoaded = true;
      }
      const baseConfigPath = path.resolve("tools/presto/");
      // Mapping the whole volume causes confusing issues, probably stemming from symlinks; for now just link each file individually.
      const mountedFiles = [
        "config.properties",
        "jvm.config",
        "catalog/memory.properties",
        "catalog/jmx.properties"
      ].map(configPath => `-v ${path.join(baseConfigPath, configPath)}:/etc/presto/${configPath}`);
      // Run the presto Docker image.
      // This running container can be interacted with via the include CLI using:
      // `docker exec -it presto presto`.
      const result = exec(
        [
          "docker run",
          "--rm",
          `--name ${DOCKER_CONTAINER_NAME}`,
          ...mountedFiles,
          `-p ${PrestoFixture.PRESTO_TEST_CREDENTIALS.port}:${PrestoFixture.PRESTO_TEST_CREDENTIALS.port}`,
          USE_CLOUD_BUILD_NETWORK ? "--network cloudbuild" : "",
          "bazel/tools/presto:presto_image"
        ].join(" ")
      );
      result.stdout.on("data", data => {
        // tslint:disable-next-line: no-console
        console.log("stdout: " + data.toString());
      });
      result.stderr.on("data", data => {
        // tslint:disable-next-line: no-console
        console.log("stderr: " + data.toString());
      });
      result.on("exit", code => {
        // tslint:disable-next-line: no-console
        console.log("child process exited with code " + code.toString());
      });

      const dbadapter = await dbadapters.create(PrestoFixture.PRESTO_TEST_CREDENTIALS, "presto");

      // Block until presto is ready to accept requests.
      await sleepUntil(async () => {
        try {
          await dbadapter.execute("select 1");
          return true;
        } catch (e) {
          // tslint:disable-next-line: no-console
          console.log("PrestoFixture -> constructor -> e", e);
          return false;
        }
      }, 500);
    });

    tearDown("stopping presto", () => {
      execSync(`docker stop ${DOCKER_CONTAINER_NAME}`);
    });
  }
}
