import * as pg from "pg";

import { execSync } from "child_process";
import { sleepUntil } from "df/common/promises";
import { IHookHandler } from "df/testing";
import { convertFieldType, PgPoolExecutor } from "df/api/utils/postgres";

const USE_CLOUD_BUILD_NETWORK = !!process.env.USE_CLOUD_BUILD_NETWORK;
const DOCKER_CONTAINER_NAME = "postgres-df-integration-testing";
const POSTGRES_SERVE_PORT = 5432;

export class PostgresFixture {
  public static readonly host = USE_CLOUD_BUILD_NETWORK ? DOCKER_CONTAINER_NAME : "localhost";

  private static imageLoaded = false;

  constructor(port: number, setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("starting postgres", async () => {
      if (!PostgresFixture.imageLoaded) {
        // Load the postgres image into the local Docker daemon.
        execSync("tools/postgres/postgres_image.executable");
        PostgresFixture.imageLoaded = true;
      }
      // Run the postgres Docker image.
      execSync(
        [
          "docker run",
          "--rm",
          `--name ${DOCKER_CONTAINER_NAME}`,
          "-e POSTGRES_PASSWORD=password",
          "-d",
          `-p ${port}:${POSTGRES_SERVE_PORT}`,
          USE_CLOUD_BUILD_NETWORK ? "--network cloudbuild" : "",
          "bazel/tools/postgres:postgres_image"
        ].join(" ")
      );

      const jdbcCredentials = {
        username: "postgres",
        databaseName: "postgres",
        password: "password",
        port,
        host: PostgresFixture.host
      };
      const clientConfig: Partial<pg.ClientConfig> = {
        user: jdbcCredentials.username,
        password: jdbcCredentials.password,
        database: jdbcCredentials.databaseName,
        ssl: false,
        port,
        host: PostgresFixture.host
      };
      const queryExecutor = new PgPoolExecutor(clientConfig);

      // Block until postgres is ready to accept requests.
      await sleepUntil(async () => {
        try {
          await queryExecutor.withClientLock(async client => {
            execute: async (
              statement: string,
              options: {
                params?: any[];
                rowLimit?: number;
                byteLimit?: number;
                includeQueryInError?: boolean;
              } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
            ) => {
              const rows = await client.execute(statement, options);
              return { rows, metadata: {} };
            };
          });
          return true;
        } catch (e) {
          return false;
        }
      });
    });

    tearDown("stopping postgres", () => {
      execSync(`docker stop ${DOCKER_CONTAINER_NAME}`);
    });
  }
}
