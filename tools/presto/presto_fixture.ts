import * as presto from "presto-client";

import { execSync } from "child_process";
import * as dbadapters from "df/api/dbadapters";
import { sleep, sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";

const USE_CLOUD_BUILD_NETWORK = !!process.env.USE_CLOUD_BUILD_NETWORK;
const DOCKER_CONTAINER_NAME = "presto-df-integration-testing";
// We remap this to something more unique.
const PRESTO_SERVE_PORT = 8080;

export const prestoClient = new presto.Client({
  host: "127.0.0.1",
  port: 1234,
  source: "df-integration-tests",
  user: "df-integration-tests",
  // These are included in the docker image.
  catalog: "tpch",
  schema: "sf1"
  // TODO: Add BIGINT json parser; see presto-client docs.
});

interface IPrestoExecutionResult {
  columns?: presto.IPrestoClientColumnMetaData[];
  data?: presto.PrestoClientColumnDatum[];
  error?: any;
  queryId?: string;
  stats?: presto.IPrestoClientStats;
}

export class PrestoFixture {
  public static readonly host = USE_CLOUD_BUILD_NETWORK ? DOCKER_CONTAINER_NAME : "localhost";

  private static imageLoaded = false;

  constructor(port: number, setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting presto", async () => {
      if (!PrestoFixture.imageLoaded) {
        // Load the presto image into the local Docker daemon.
        execSync("tools/presto/presto_image.executable");
        PrestoFixture.imageLoaded = true;
      }
      // Run the presto Docker image.
      // This running container can be interacted with via the include CLI using:
      // `docker exec -it presto presto --catalog tpch --schema sf1`
      // (Catalog and schema conform to the default provided within the image).
      execSync(
        [
          "docker run",
          "--rm",
          `--name ${DOCKER_CONTAINER_NAME}`,
          "-d",
          `-p ${port}:${PRESTO_SERVE_PORT}`,
          USE_CLOUD_BUILD_NETWORK ? "--network cloudbuild" : "",
          "bazel/tools/presto:presto_image"
        ].join(" ")
      );

      // Block until presto is ready to accept requests.
      await sleepUntil(async () => {
        try {
          await prestoExecute({ query: "select 1" });
          return true;
        } catch (e) {
          return false;
        }
      });
    });

    tearDown("stopping presto", () => {
      execSync(`docker stop ${DOCKER_CONTAINER_NAME}`);
    });
  }
}

export function prestoExecute(
  executeOptions: presto.IPrestoClientExecuteOptions
): Promise<IPrestoExecutionResult> {
  const result: IPrestoExecutionResult = {};
  return new Promise((resolve, reject) => {
    prestoClient.execute({
      ...executeOptions,
      cancel: () => {
        return false;
      },
      state: (error: any, queryId: string, stats: presto.IPrestoClientStats) => {
        result.error = error;
        result.queryId = queryId;
        result.stats = stats;
      },
      columns: (error: any, columns: presto.IPrestoClientColumnMetaData[]) => {
        result.error = error;
        result.columns = columns;
      },
      data: (
        error: any,
        data: presto.PrestoClientColumnDatum[],
        columns: presto.IPrestoClientColumnMetaData[],
        stats: presto.IPrestoClientStats
      ) => {
        result.error = error;
        result.data = data;
        result.columns = columns;
        result.stats = stats;
      },
      success: (error: any, stats: presto.IPrestoClientStats) => {
        if (!!error) {
          reject(error);
        }
        result.stats = stats;
        resolve(result);
      },
      error: (error: any) => {
        reject(error);
      }
    });
  });
}
