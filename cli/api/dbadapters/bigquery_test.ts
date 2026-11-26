import { BigQuery, Dataset, Table, TableField, TableMetadata } from "@google-cloud/bigquery";
import { expect } from "chai";
import { EventEmitter } from "events";
import { AuthClient, gaxios, Impersonated } from "google-auth-library";
import { anything, instance, mock, when } from "ts-mockito";

import {
  BigQueryDbAdapter,
  createAuthClient,
  createBigQueryClientProvider,
  createImpersonatedClient,
  DeferredAuthClient,
  getBigQueryClientOptions,
} from "df/cli/api/dbadapters/bigquery";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

const TARGET_SERVICE_ACCOUNT = "target-sa@target-project.iam.gserviceaccount.com";
const TEST_POLICY_TAG = "projects/test-project/locations/us/taxonomies/1/policyTags/2";
const OTHER_POLICY_TAG = "projects/test-project/locations/us/taxonomies/1/policyTags/3";

async function setMetadataAndCaptureSchema(
  initialFields: TableField[],
  columns: dataform.IColumnDescriptor[],
): Promise<TableField[]> {
  const mockBigQuery = mock<BigQuery>();
  const mockDataset = mock<Dataset>();
  const mockTable = mock<Table>();
  let capturedSchema: TableField[] = [];

  when(mockBigQuery.dataset("sch")).thenReturn(instance(mockDataset));
  when(mockDataset.table("tab")).thenReturn(instance(mockTable));
  when(mockTable.getMetadata()).thenResolve([{ schema: { fields: initialFields } }, undefined]);
  when(mockTable.setMetadata(anything())).thenCall(async (metadata: TableMetadata) => {
    capturedSchema = metadata.schema?.fields ?? (metadata.schema as TableField[]);
    return [{}, undefined];
  });

  const adapter = new BigQueryDbAdapter(
    dataform.BigQuery.create({ projectId: "p", location: "US" }),
    {
      concurrencyLimit: 1,
      clientProvider: () => instance(mockBigQuery),
    },
  );

  await adapter.setMetadata(
    dataform.ExecutionAction.create({
      target: { database: "db", schema: "sch", name: "tab" },
      actionDescriptor: { columns },
    }),
  );

  return capturedSchema;
}

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
          schema: {
            fields: [
              { name: "col1", type: "STRING", mode: "NULLABLE" },
              { name: "bn", type: "BIGNUMERIC" },
              { name: "n", type: "NUMERIC" },
              { name: "j", type: "JSON" },
              { name: "iv", type: "INTERVAL" },
            ],
          },
          lastModifiedTime: "123456789",
        },
      ] as any),
    );

    const result = await adapter.tables(projectId, schemaName);

    expect(result.length).to.equal(1);
    expect(result[0].target.database).to.equal(projectId);
    expect(result[0].target.schema).to.equal(schemaName);
    expect(result[0].target.name).to.equal(tableName);
    expect(result[0].fields.map((field) => [field.name, field.primitive])).deep.equals([
      ["col1", dataform.Field.Primitive.STRING],
      ["bn", dataform.Field.Primitive.BIGNUMERIC],
      ["n", dataform.Field.Primitive.NUMERIC],
      ["j", dataform.Field.Primitive.JSON],
      ["iv", dataform.Field.Primitive.INTERVAL],
    ]);
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

  test("setMetadata distinguishes flat column from nested column when flat column is listed first", async () => {
    const schema = await setMetadataAndCaptureSchema(
      [
        { name: "userssn", type: "STRING" },
        { name: "user", type: "RECORD", fields: [{ name: "ssn", type: "STRING" }] },
      ],
      [
        { path: ["userssn"], description: "flat column" },
        { path: ["user"], description: "user record" },
        {
          path: ["user", "ssn"],
          description: "nested column",
          bigqueryPolicyTags: [TEST_POLICY_TAG],
        },
      ],
    );

    expect(schema).to.deep.equal([
      { name: "userssn", type: "STRING", description: "flat column" },
      {
        name: "user",
        type: "RECORD",
        description: "user record",
        fields: [
          {
            name: "ssn",
            type: "STRING",
            description: "nested column",
            policyTags: { names: [TEST_POLICY_TAG] },
          },
        ],
      },
    ]);
  });

  test("setMetadata distinguishes flat column from nested column when nested column is listed first", async () => {
    const schema = await setMetadataAndCaptureSchema(
      [
        { name: "userssn", type: "STRING" },
        { name: "user", type: "RECORD", fields: [{ name: "ssn", type: "STRING" }] },
      ],
      [
        {
          path: ["user", "ssn"],
          description: "nested column",
          bigqueryPolicyTags: [TEST_POLICY_TAG],
        },
        { path: ["userssn"], description: "flat column" },
      ],
    );

    expect(schema).to.deep.equal([
      { name: "userssn", type: "STRING", description: "flat column" },
      {
        name: "user",
        type: "RECORD",
        fields: [
          {
            name: "ssn",
            type: "STRING",
            description: "nested column",
            policyTags: { names: [TEST_POLICY_TAG] },
          },
        ],
      },
    ]);
  });

  test("setMetadata distinguishes nested columns with different segment boundaries", async () => {
    const schema = await setMetadataAndCaptureSchema(
      [
        { name: "ab", type: "RECORD", fields: [{ name: "c", type: "STRING" }] },
        { name: "a", type: "RECORD", fields: [{ name: "bc", type: "STRING" }] },
      ],
      [
        { path: ["ab", "c"], description: "ab.c desc", bigqueryPolicyTags: [TEST_POLICY_TAG] },
        { path: ["a", "bc"], description: "a.bc desc", bigqueryPolicyTags: [OTHER_POLICY_TAG] },
      ],
    );

    expect(schema).to.deep.equal([
      {
        name: "ab",
        type: "RECORD",
        fields: [
          {
            name: "c",
            type: "STRING",
            description: "ab.c desc",
            policyTags: { names: [TEST_POLICY_TAG] },
          },
        ],
      },
      {
        name: "a",
        type: "RECORD",
        fields: [
          {
            name: "bc",
            type: "STRING",
            description: "a.bc desc",
            policyTags: { names: [OTHER_POLICY_TAG] },
          },
        ],
      },
    ]);
  });

  test("setMetadata applies a record description and nested field policy tags", async () => {
    const schema = await setMetadataAndCaptureSchema(
      [{ name: "user", type: "RECORD", fields: [{ name: "ssn", type: "STRING" }] }],
      [
        { path: ["user"], description: "user record" },
        {
          path: ["user", "ssn"],
          description: "nested ssn",
          bigqueryPolicyTags: [TEST_POLICY_TAG],
        },
      ],
    );

    expect(schema).to.deep.equal([
      {
        name: "user",
        type: "RECORD",
        description: "user record",
        fields: [
          {
            name: "ssn",
            type: "STRING",
            description: "nested ssn",
            policyTags: { names: [TEST_POLICY_TAG] },
          },
        ],
      },
    ]);
  });

  test("setMetadata ignores a column descriptor without a path", async () => {
    const schema = await setMetadataAndCaptureSchema(
      [{ name: "id", type: "INTEGER" }],
      [
        { description: "missing path", bigqueryPolicyTags: [TEST_POLICY_TAG] },
        { path: ["id"], description: "id desc" },
      ],
    );

    expect(schema).to.deep.equal([{ name: "id", type: "INTEGER", description: "id desc" }]);
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

  suite("executeRaw", () => {
    interface IFakePage {
      rows: any[];
      apiResponse?: any;
    }

    /**
     * A fake BigQuery client that answers a query with the first page and the requests for its
     * next pages with the following ones. It records the arguments of every call.
     */
    function fakeBigQueryReturning(pages: IFakePage[]) {
      const queryArguments: any[][] = [];
      const nextPageOptions: any[] = [];
      let pagesReturned = 0;
      const job = {
        getQueryResults: async (options: any): Promise<any[]> => {
          nextPageOptions.push(options);
          return nextAnswer();
        },
      };
      function nextAnswer(): any[] {
        const page = pages[pagesReturned++];
        const nextPage =
          pagesReturned < pages.length ? { job, pageToken: `page-${pagesReturned}` } : null;
        return [page.rows, nextPage, page.apiResponse ?? {}];
      }
      const client: any = {
        query: async (...args: any[]): Promise<any[]> => {
          queryArguments.push(args);
          return nextAnswer();
        },
      };
      return { client, queryArguments, nextPageOptions };
    }

    function adapterFor(client: any) {
      return new BigQueryDbAdapter(dataform.BigQuery.create({ projectId: "p", location: "US" }), {
        clientProvider: () => client,
      });
    }

    const row = (value: string) => ({ f: [{ v: value }] });

    test("passes rowLimit to the query and asks the client for a single page", async () => {
      const fake = fakeBigQueryReturning([{ rows: [row("1"), row("2")] }]);

      const result = await adapterFor(fake.client).executeRaw("SELECT 1", { rowLimit: 2 });

      expect(result.rows).to.deep.equal([row("1"), row("2")]);
      const [query, queryOptions] = fake.queryArguments[0];
      expect(query.maxResults).to.equal(2);
      expect(queryOptions).to.deep.equal({ maxResults: 2 });
      expect(fake.nextPageOptions).to.deep.equal([]);
    });

    test("waits for a query that is still running when rowLimit is set", async () => {
      const fake = fakeBigQueryReturning([
        { rows: [], apiResponse: { jobComplete: false } },
        {
          rows: [row("1"), row("2")],
          apiResponse: { jobComplete: true, schema: { fields: [{ name: "x", type: "INTEGER" }] } },
        },
      ]);

      const result = await adapterFor(fake.client).executeRaw("SELECT 1", { rowLimit: 2 });

      expect(result.rows).to.deep.equal([row("1"), row("2")]);
      expect(result.schema.map((field) => field.name)).to.deep.equal(["x"]);
    });

    test("fetches the next pages until rowLimit is reached", async () => {
      const fake = fakeBigQueryReturning([
        { rows: [row("1")], apiResponse: { schema: { fields: [{ name: "x", type: "INTEGER" }] } } },
        { rows: [row("2"), row("3")] },
        { rows: [row("4"), row("5")] },
        { rows: [row("6")] },
      ]);

      const result = await adapterFor(fake.client).executeRaw("SELECT 1", { rowLimit: 4 });

      expect(result.rows).to.deep.equal([row("1"), row("2"), row("3"), row("4")]);
      expect(result.schema.map((field) => field.name)).to.deep.equal(["x"]);
      expect(
        fake.nextPageOptions.map(({ pageToken, maxResults }) => ({ pageToken, maxResults })),
      ).to.deep.equal([
        { pageToken: "page-1", maxResults: 3 },
        { pageToken: "page-2", maxResults: 1 },
      ]);
    });

    test("returns fewer rows than rowLimit when the query has no more", async () => {
      const fake = fakeBigQueryReturning([{ rows: [row("1")] }, { rows: [row("2")] }]);

      const result = await adapterFor(fake.client).executeRaw("SELECT 1", { rowLimit: 5 });

      expect(result.rows).to.deep.equal([row("1"), row("2")]);
    });

    test("passes maximumBytesBilled to the query as a string", async () => {
      const fake = fakeBigQueryReturning([{ rows: [] }]);

      await adapterFor(fake.client).executeRaw("SELECT 1", { maximumBytesBilled: 1048576 });

      expect(fake.queryArguments[0][0].maximumBytesBilled).to.equal("1048576");
    });

    for (const [description, options] of [
      ["no options are given", undefined],
      ["the limits are unset", {}],
      ["the query is a dry run", { rowLimit: 2, bigquery: { dryRun: true } }],
    ] as const) {
      test(`lets the client fetch every page when ${description}`, async () => {
        const fake = fakeBigQueryReturning([{ rows: [row("1")] }]);

        const result = await adapterFor(fake.client).executeRaw("SELECT 1", options);

        expect(result.rows).to.deep.equal([row("1")]);
        expect(fake.queryArguments[0]).to.have.lengthOf(1);
        expect(fake.queryArguments[0][0].maximumBytesBilled).to.equal(undefined);
        expect(fake.nextPageOptions).to.deep.equal([]);
      });
    }
  });
});
