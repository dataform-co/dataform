import { Dataset, Table } from "@google-cloud/bigquery";
import { expect } from "chai";
import { EventEmitter } from "events";
import { AuthClient, gaxios, Impersonated } from "google-auth-library";
import Long from "long";
import { anything, instance, mock, when } from "ts-mockito";

import {
  BigQueryDbAdapter,
  createAuthClient,
  createBigQueryClientProvider,
  createImpersonatedClient,
  DeferredAuthClient,
  getBigQueryClientOptions,
} from "df/cli/api/dbadapters/bigquery";
import { prettyJsonStringify } from "df/cli/api/utils";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

const TARGET_SERVICE_ACCOUNT = "target-sa@target-project.iam.gserviceaccount.com";

/** A source AuthClient that records its requests and answers them with an impersonated token. */
class FakeSourceClient extends AuthClient {
  public readonly requests: gaxios.GaxiosOptions[] = [];

  public async getAccessToken() {
    return { token: "source-token" };
  }

  public async getRequestHeaders() {
    return new Headers({ authorization: "Bearer source-token" });
  }

  public async request<T>(options: gaxios.GaxiosOptions): gaxios.GaxiosPromise<T> {
    this.requests.push(options);
    return {
      data: { accessToken: "impersonated-token", expireTime: "2099-01-01T00:00:00Z" },
    } as any;
  }
}

suite("BigQueryDbAdapter", () => {
  suite("getBigQueryClientOptions", () => {
    test("passes the parsed service account key to the client", () => {
      const serviceAccountKey = {
        type: "service_account",
        project_id: "key-project-id",
        private_key_id: "key-id-123",
        private_key: "fake-private-key",
        client_email: "test-sa@key-project-id.iam.gserviceaccount.com",
        client_id: "1234567890",
      };
      const options = getBigQueryClientOptions(
        dataform.BigQuery.create({
          projectId: "target-project-id",
          location: "EU",
          credentials: JSON.stringify(serviceAccountKey),
        }),
      );
      expect(options).to.deep.include({
        projectId: "target-project-id",
        location: "EU",
        credentials: serviceAccountKey,
      });
    });

    test("leaves out credentials when no key is given, so ADC is used", () => {
      const credentials = dataform.BigQuery.create({ projectId: "adc-project-id", location: "US" });
      const options = getBigQueryClientOptions(
        credentials,
        undefined,
        createAuthClient(credentials),
      );
      expect(options).to.deep.include({ projectId: "adc-project-id", location: "US" });
      expect(Boolean(options.credentials)).to.equal(false);
      expect(options.authClient).to.equal(undefined);
    });

    test("uses an impersonated auth client when a service account to impersonate is given", () => {
      const credentials = dataform.BigQuery.create({
        projectId: "target-project-id",
        location: "US",
        impersonateServiceAccount: TARGET_SERVICE_ACCOUNT,
      });
      const options = getBigQueryClientOptions(
        credentials,
        undefined,
        createAuthClient(credentials),
      );
      expect(options).to.deep.include({ projectId: "target-project-id", location: "US" });
      expect(options.authClient).to.be.instanceOf(Impersonated);
      expect((options.authClient as Impersonated).getTargetPrincipal()).to.equal(
        TARGET_SERVICE_ACCOUNT,
      );
      expect(Boolean(options.credentials)).to.equal(false);
    });

    test("uses the requested project instead of the default one", () => {
      const options = getBigQueryClientOptions(
        dataform.BigQuery.create({ projectId: "default-project", location: "US" }),
        "other-project",
      );
      expect(options.projectId).to.equal("other-project");
    });

    test("leaves the location undefined when it is not set, so BigQuery auto-selects it", () => {
      // Unset proto3 string fields read as "", which BigQuery rejects as a job location.
      const credentials = dataform.BigQuery.create({ projectId: "default-project" });
      expect(credentials.location).to.equal("");
      expect(getBigQueryClientOptions(credentials).location).to.equal(undefined);
      expect(createBigQueryClientProvider(credentials)().location).to.equal(undefined);
    });

    test("throws on malformed service account key JSON", () => {
      expect(() =>
        getBigQueryClientOptions({
          projectId: "target-project-id",
          location: "US",
          credentials: "{not-valid-json",
        }),
      ).to.throw(SyntaxError);
    });
  });

  test("createBigQueryClientProvider creates and caches one client per project", () => {
    const clientProvider = createBigQueryClientProvider(
      dataform.BigQuery.create({ projectId: "default-project", location: "EU" }),
    );
    const defaultClient = clientProvider();
    expect(defaultClient.projectId).to.equal("default-project");
    expect(defaultClient.location).to.equal("EU");
    expect(clientProvider()).to.equal(defaultClient);
    expect(clientProvider("other-project").projectId).to.equal("other-project");
  });

  test("createBigQueryClientProvider shares one impersonated auth client between projects", async () => {
    const clientProvider = createBigQueryClientProvider(
      dataform.BigQuery.create({
        projectId: "default-project",
        location: "US",
        impersonateServiceAccount: TARGET_SERVICE_ACCOUNT,
      }),
    );
    const defaultAuthClient = await clientProvider().authClient.getClient();
    expect(defaultAuthClient).to.be.instanceOf(Impersonated);
    expect((defaultAuthClient as Impersonated).getTargetPrincipal()).to.equal(
      TARGET_SERVICE_ACCOUNT,
    );
    expect(await clientProvider("other-project").authClient.getClient()).to.equal(
      defaultAuthClient,
    );
  });

  test("impersonated client exchanges the lazily resolved source credentials for a token", async () => {
    const sourceClient = new FakeSourceClient();
    let sourceResolved = false;
    const impersonatedClient = createImpersonatedClient(
      TARGET_SERVICE_ACCOUNT,
      new DeferredAuthClient(async () => {
        sourceResolved = true;
        return sourceClient;
      }),
    );
    expect(sourceResolved).to.equal(false);

    const { token } = await impersonatedClient.getAccessToken();

    expect(token).to.equal("impersonated-token");
    expect(sourceResolved).to.equal(true);
    expect(sourceClient.requests.length).to.equal(1);
    expect(sourceClient.requests[0]).to.deep.include({
      method: "POST",
      url: `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${TARGET_SERVICE_ACCOUNT}:generateAccessToken`,
      data: {
        delegates: [],
        scope: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/drive",
        ],
        lifetime: "3600s",
      },
    });
  });

  test("tables() with schema filters correctly", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();
    const mockTable = mock<Table>();

    const tableName = "table1";
    const schemaName = "schema1";
    const projectId = "project1";

    const credentials = dataform.BigQuery.create({ projectId, location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, {
      clientProvider: () => instance(mockBigQuery),
    });

    when(mockBigQuery.dataset(schemaName)).thenReturn(instance(mockDataset));
    // getTables returns an array where the first element is an array of tables.
    // Each table object needs an 'id' property.
    when(mockDataset.getTables(anything())).thenReturn(
      Promise.resolve([[{ id: tableName }]] as any),
    );
    when(mockDataset.table(tableName)).thenReturn(instance(mockTable));
    when(mockTable.getMetadata()).thenReturn(
      Promise.resolve([
        {
          type: "TABLE",
          tableReference: { projectId, datasetId: schemaName, tableId: tableName },
          schema: { fields: [{ name: "col1", type: "STRING", mode: "NULLABLE" }] },
          lastModifiedTime: "123456789",
        },
      ] as any),
    );

    const result = await adapter.tables(projectId, schemaName);

    expect(result.length).to.equal(1);
    expect(result[0].target.database).to.equal(projectId);
    expect(result[0].target.schema).to.equal(schemaName);
    expect(result[0].target.name).to.equal(tableName);
    expect(result[0].fields.length).to.equal(1);
    expect(result[0].fields[0].name).to.equal("col1");
  });

  test("tables() without schema lists all datasets and tables", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();
    const mockTable = mock<Table>();
    const schemaName = "schema1";
    const tableName = "table1";
    const projectId = "project";

    const credentials = dataform.BigQuery.create({ projectId, location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, {
      clientProvider: () => instance(mockBigQuery),
    });

    when(mockBigQuery.dataset(schemaName)).thenReturn(instance(mockDataset));
    when(mockDataset.getTables(anything())).thenReturn(
      Promise.resolve([[{ id: tableName }]] as any),
    );
    when(mockDataset.table(tableName)).thenReturn(instance(mockTable));
    when(mockTable.getMetadata()).thenReturn(
      Promise.resolve([
        {
          type: "TABLE",
          tableReference: { projectId, datasetId: schemaName, tableId: tableName },
          schema: { fields: [{ name: "col1", type: "STRING" }] },
          lastModifiedTime: "123456789",
        },
      ] as any),
    );

    when(mockBigQuery.getDatasets(anything())).thenReturn(
      Promise.resolve([[{ id: schemaName }]] as any),
    );

    const result = await adapter.tables(projectId);

    expect(result.length).to.equal(1);
    expect(result[0].target.database).to.equal(projectId);
    expect(result[0].target.schema).to.equal(schemaName);
    expect(result[0].target.name).to.equal(tableName);
  });

  test("tables() returns empty list when dataset does not exist yet", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();

    const adapter = new BigQueryDbAdapter(
      dataform.BigQuery.create({ projectId: "project1", location: "US" }),
      { clientProvider: () => instance(mockBigQuery) },
    );

    when(mockBigQuery.dataset("missing_schema")).thenReturn(instance(mockDataset));
    when(mockDataset.getTables(anything())).thenReturn(
      Promise.reject({ errors: [{ reason: "notFound" }] }),
    );

    const result = await adapter.tables("project1", "missing_schema");

    expect(result).to.deep.equal([]);
  });

  test("tables() rethrows errors other than notFound", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();

    const adapter = new BigQueryDbAdapter(
      dataform.BigQuery.create({ projectId: "project1", location: "US" }),
      { clientProvider: () => instance(mockBigQuery) },
    );

    when(mockBigQuery.dataset("restricted_schema")).thenReturn(instance(mockDataset));
    when(mockDataset.getTables(anything())).thenReturn(
      Promise.reject(new Error("Access Denied: Dataset project1:restricted_schema")),
    );

    let caughtError: Error | undefined;
    try {
      await adapter.tables("project1", "restricted_schema");
    } catch (e) {
      caughtError = e;
    }
    expect(caughtError?.message).to.equal("Access Denied: Dataset project1:restricted_schema");
  });

  test("setMetadata handles action without columns", async () => {
    // Partial mock for BigQuery client to avoid real network calls
    const mockBigQuery: any = {
      dataset: () => ({
        table: () => ({
          getMetadata: () => Promise.resolve([{ schema: { fields: [] } }]),
          setMetadata: (metadata: any) => {
            expect(metadata.description).to.equal("test");
            return Promise.resolve([]);
          },
        }),
      }),
    };

    const credentials = dataform.BigQuery.create({ projectId: "p", location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, {
      concurrencyLimit: 1,
      clientProvider: () => mockBigQuery,
    });

    const action = dataform.ExecutionAction.create({
      target: { database: "db", schema: "sch", name: "tab" },
      actionDescriptor: { description: "test" },
      // columns is missing/null in this action
    });

    // This should not throw "cannot read property 'find' of undefined"
    await adapter.setMetadata(action);
  });

  test("setMetadata correctly maps column descriptions", async () => {
    const mockBigQuery: any = {
      dataset: () => ({
        table: () => ({
          getMetadata: () =>
            Promise.resolve([
              {
                schema: {
                  fields: [{ name: "id", type: "INTEGER" }],
                },
              },
            ]),
          setMetadata: (metadata: any) => {
            expect(metadata.schema[0].description).to.equal("id desc");
            return Promise.resolve([]);
          },
        }),
      }),
    };

    const credentials = dataform.BigQuery.create({ projectId: "p", location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, {
      concurrencyLimit: 1,
      clientProvider: () => mockBigQuery,
    });

    const action = dataform.ExecutionAction.create({
      target: { database: "db", schema: "sch", name: "tab" },
      actionDescriptor: {
        columns: [{ path: ["id"], description: "id desc" }],
      },
    });

    await adapter.setMetadata(action);
  });

  test("search() filters out tables whose metadata is not found", async () => {
    const projectId = "project1";
    const liveSchema = "live_schema";
    const deletedSchema = "deleted_schema";
    const tableName = "table1";

    const mockBigQuery: any = {
      createQueryStream: () => {
        const stream = new EventEmitter();
        process.nextTick(() => {
          stream.emit("data", {
            table_catalog: projectId,
            table_schema: liveSchema,
            table_name: tableName,
          });
          stream.emit("data", {
            table_catalog: projectId,
            table_schema: deletedSchema,
            table_name: tableName,
          });
          stream.emit("end");
        });
        return stream;
      },
      dataset: (schema: string) => ({
        table: (name: string) => ({
          getMetadata: () => {
            if (schema === deletedSchema) {
              return Promise.reject({ errors: [{ reason: "notFound" }] });
            }
            return Promise.resolve([
              {
                type: "TABLE",
                tableReference: { projectId, datasetId: schema, tableId: name },
                schema: { fields: [{ name: "col1", type: "STRING" }] },
                lastModifiedTime: "123456789",
              },
            ]);
          },
        }),
      }),
    };

    const credentials = dataform.BigQuery.create({ projectId, location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, {
      clientProvider: () => mockBigQuery,
    });

    const result = await adapter.search("schema");

    expect(result.length).to.equal(1);
    expect(result[0].target).to.eql({
      database: projectId,
      schema: liveSchema,
      name: tableName,
    });
  });

  suite("dry runs", () => {
    function dryRunAdapter(
      jobMetadata: any,
      onCreateQueryJob: (options: any) => void = () => undefined,
    ) {
      let resultsRead = false;
      const mockBigQuery: any = {
        createQueryJob: async (options: any) => {
          onCreateQueryJob(options);
          return [
            {
              metadata: jobMetadata,
              getQueryResultsStream: () => {
                resultsRead = true;
                // Like BigQuery: dry-run jobs aren't saved, so reading their results fails.
                let ended = false;
                const stream = Object.assign(new EventEmitter(), { end: () => (ended = true) });
                process.nextTick(() => {
                  if (!ended) {
                    stream.emit("error", new Error("Not found: Job"));
                  }
                });
                return stream;
              },
            },
          ];
        },
      };
      const adapter = new BigQueryDbAdapter(dataform.BigQuery.create({ projectId: "p" }), {
        clientProvider: () => mockBigQuery,
      });
      return { adapter, resultsRead: () => resultsRead };
    }

    test("returns the job statistics without reading the results", async () => {
      let dryRunOption: boolean;
      const { adapter, resultsRead } = dryRunAdapter(
        {
          jobReference: { projectId: "p", location: "US" },
          statistics: {
            query: {
              statementType: "CREATE_TABLE_AS_SELECT",
              totalBytesProcessed: "2650191",
              referencedTables: [
                { projectId: "bigquery-public-data", datasetId: "samples", tableId: "shakespeare" },
              ],
              schema: {
                fields: [
                  { name: "word", type: "STRING", mode: "NULLABLE" },
                  { name: "tags", type: "STRING", mode: "REPEATED" },
                  {
                    name: "stats",
                    type: "RECORD",
                    fields: [{ name: "c", type: "INTEGER" }],
                  },
                ],
              },
            },
          },
        },
        (options) => (dryRunOption = options.dryRun),
      );

      const result = await adapter.execute("CREATE TABLE t AS SELECT 1", {
        bigquery: { dryRun: true },
      });

      expect(dryRunOption).equals(true);
      expect(resultsRead()).equals(false);
      expect(result.rows).deep.equals([]);
      expect(result.metadata.bigquery.jobId).equals(undefined);
      expect(result.metadata.bigquery.totalBytesProcessed.toString()).equals("2650191");
      // Proto JSON form, which renders int64 as strings. `run --json` prints Long objects instead;
      // see the precision test.
      expect(dataform.ExecutionMetadata.create(result.metadata).toJSON()).deep.equals({
        bigquery: {
          totalBytesProcessed: "2650191",
          referencedTables: [
            { database: "bigquery-public-data", schema: "samples", name: "shakespeare" },
          ],
          schema: [
            { name: "word", primitive: "STRING" },
            { name: "tags", primitive: "STRING", flags: ["REPEATED"] },
            { name: "stats", struct: { fields: [{ name: "c", primitive: "INTEGER" }] } },
          ],
        },
      });
    });

    test("handles statistics without referenced tables or schema", async () => {
      // As returned for scripts whose estimation stops at a DDL statement.
      const { adapter } = dryRunAdapter({
        statistics: {
          query: {
            statementType: "SCRIPT",
            totalBytesProcessed: "0",
            totalBytesProcessedAccuracy: "LOWER_BOUND",
          },
        },
      });

      const result = await adapter.execute("CREATE TEMP TABLE t AS SELECT 1; SELECT * FROM t", {
        bigquery: { dryRun: true },
      });

      expect(result.rows).deep.equals([]);
      expect(result.metadata.bigquery.totalBytesProcessed.toString()).equals("0");
      expect(result.metadata.bigquery.referencedTables).deep.equals([]);
      expect(result.metadata.bigquery.schema).deep.equals([]);
    });

    test("keeps the full precision of large byte counts", async () => {
      // Above both 2^32 and Number.MAX_SAFE_INTEGER.
      const { adapter } = dryRunAdapter({
        statistics: { query: { totalBytesProcessed: "9007199254740993" } },
      });

      const result = await adapter.execute("SELECT 1", { bigquery: { dryRun: true } });

      expect(result.metadata.bigquery.totalBytesProcessed.toString()).equals("9007199254740993");
      // As printed by `run --json`.
      const printed = JSON.parse(prettyJsonStringify({ metadata: result.metadata }));
      expect(Object.keys(printed.metadata.bigquery.totalBytesProcessed)).to.have.members([
        "low",
        "high",
        "unsigned",
      ]);
      expect(Long.fromValue(printed.metadata.bigquery.totalBytesProcessed).toString()).equals(
        "9007199254740993",
      );
    });

    test("reports 0 for byte counts BigQuery didn't report, like real runs", async () => {
      for (const jobMetadata of [{}, { statistics: {} }, { statistics: { query: {} } }]) {
        const { adapter } = dryRunAdapter(jobMetadata);

        const result = await adapter.execute("SELECT 1", { bigquery: { dryRun: true } });

        expect(JSON.parse(prettyJsonStringify(result.metadata))).deep.equals({
          bigquery: {
            totalBytesProcessed: { low: 0, high: 0, unsigned: false },
            referencedTables: [],
            schema: [],
          },
        });
      }
    });

    test("maps nested repeated records and RANGE as UNKNOWN", async () => {
      const { adapter } = dryRunAdapter({
        statistics: {
          query: {
            totalBytesProcessed: "0",
            schema: {
              fields: [
                {
                  name: "events",
                  type: "RECORD",
                  mode: "REPEATED",
                  fields: [
                    { name: "amount", type: "NUMERIC" },
                    {
                      name: "items",
                      type: "STRUCT",
                      mode: "REPEATED",
                      fields: [{ name: "tags", type: "STRING", mode: "REPEATED" }],
                    },
                  ],
                },
                { name: "period", type: "RANGE", rangeElementType: { type: "DATE" } },
              ],
            },
          },
        },
      });

      const result = await adapter.execute("SELECT 1", { bigquery: { dryRun: true } });

      expect(
        dataform.ExecutionMetadata.create(result.metadata).toJSON().bigquery.schema,
      ).deep.equals([
        {
          name: "events",
          flags: ["REPEATED"],
          struct: {
            fields: [
              { name: "amount", primitive: "NUMERIC" },
              {
                name: "items",
                flags: ["REPEATED"],
                struct: {
                  fields: [{ name: "tags", primitive: "STRING", flags: ["REPEATED"] }],
                },
              },
            ],
          },
        },
        { name: "period", primitive: "UNKNOWN" },
      ]);
    });

    test("propagates errors from creating the job without retrying them", async () => {
      let attempts = 0;
      const { adapter, resultsRead } = dryRunAdapter({ statistics: { query: {} } }, () => {
        attempts++;
        throw new Error("Syntax error: Unexpected end of script at [1:7]");
      });

      let caughtError: Error;
      try {
        await adapter.execute("SELECT", { bigquery: { dryRun: true } });
      } catch (e) {
        caughtError = e;
      }

      expect(caughtError?.message).equals("Syntax error: Unexpected end of script at [1:7]");
      expect(attempts).equals(1);
      expect(resultsRead()).equals(false);
    });

    test("retries creating the job after a retryable error", async () => {
      let attempts = 0;
      const { adapter, resultsRead } = dryRunAdapter(
        { statistics: { query: { totalBytesProcessed: "42" } } },
        () => {
          attempts++;
          if (attempts === 1) {
            throw new Error("An internal error occurred. Retrying the job may solve the problem.");
          }
        },
      );

      const result = await adapter.execute("SELECT 1", { bigquery: { dryRun: true } });

      expect(attempts).equals(2);
      expect(result.metadata.bigquery.totalBytesProcessed.toString()).equals("42");
      expect(resultsRead()).equals(false);
    });

    test("rejects when cancelled while the job is being created", async () => {
      let cancel: () => void;
      const { adapter } = dryRunAdapter({ statistics: { query: {} } }, () => cancel());

      let caughtError: Error;
      try {
        await adapter.execute("SELECT 1", {
          bigquery: { dryRun: true },
          onCancel: (handleCancel) => (cancel = handleCancel),
        });
      } catch (e) {
        caughtError = e;
      }

      expect(caughtError?.message).equals("Query cancelled.");
    });
  });

  suite("real runs", () => {
    function realRunAdapter(jobMetadata: any) {
      const mockBigQuery: any = {
        createQueryJob: async () => [
          {
            getQueryResultsStream: () => {
              const stream = Object.assign(new EventEmitter(), { end: () => undefined });
              process.nextTick(() => {
                stream.emit("data", { id: 1 });
                stream.emit("end");
              });
              return stream;
            },
            getMetadata: async () => [jobMetadata],
          },
        ],
      };
      return new BigQueryDbAdapter(dataform.BigQuery.create({ projectId: "p" }), {
        clientProvider: () => mockBigQuery,
      });
    }

    test("reports referenced tables and schema with the job ID and byte counts", async () => {
      const adapter = realRunAdapter({
        jobReference: { projectId: "p", jobId: "dataform-123", location: "US" },
        status: { state: "DONE" },
        statistics: {
          query: {
            statementType: "CREATE_TABLE_AS_SELECT",
            totalBytesProcessed: "2650191",
            totalBytesBilled: "10485760",
            referencedTables: [
              { projectId: "bigquery-public-data", datasetId: "samples", tableId: "shakespeare" },
            ],
            schema: { fields: [{ name: "word", type: "STRING" }] },
          },
        },
      });

      const result = await adapter.execute("CREATE TABLE t AS SELECT 1");

      expect(result.rows).deep.equals([{ id: 1 }]);
      expect(dataform.ExecutionMetadata.create(result.metadata).toJSON()).deep.equals({
        bigquery: {
          jobId: "dataform-123",
          totalBytesProcessed: "2650191",
          totalBytesBilled: "10485760",
          referencedTables: [
            { database: "bigquery-public-data", schema: "samples", name: "shakespeare" },
          ],
          schema: [{ name: "word", primitive: "STRING" }],
        },
      });
    });

    test("leaves out what BigQuery didn't report", async () => {
      // As returned for a real MERGE, which reports no schema.
      const adapter = realRunAdapter({
        jobReference: { projectId: "p", jobId: "dataform-456", location: "US" },
        status: { state: "DONE" },
        statistics: {
          query: {
            statementType: "MERGE",
            referencedTables: [{ projectId: "p", datasetId: "s", tableId: "t" }],
          },
        },
      });

      const result = await adapter.execute("MERGE s.t USING (SELECT 1) ON FALSE");

      expect(JSON.parse(prettyJsonStringify(result.metadata))).deep.equals({
        bigquery: {
          jobId: "dataform-456",
          totalBytesBilled: { low: 0, high: 0, unsigned: false },
          totalBytesProcessed: { low: 0, high: 0, unsigned: false },
          referencedTables: [{ database: "p", schema: "s", name: "t" }],
          schema: [],
        },
      });
    });
  });
});
