import { BigQuery, Dataset, Table, TableField, TableMetadata } from "@google-cloud/bigquery";
import { expect } from "chai";
import { EventEmitter } from "events";
import { Impersonated } from "google-auth-library";
import Long from "long";
import { anything, instance, mock, when } from "ts-mockito";

import {
  BIGQUERY_EXTRA_SCOPES,
  BigQueryDbAdapter,
  createAuthClient,
  createBigQueryClientProvider,
  createImpersonatedClient,
  DeferredAuthClient,
  getBigQueryClientOptions,
} from "df/cli/api/dbadapters/bigquery";
import { FakeSourceClient } from "df/cli/api/testing/fake_source_client";
import { prettyJsonStringify } from "df/cli/api/utils";
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
        scopes: ["https://www.googleapis.com/auth/drive.readonly"],
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
      expect(options.scopes).to.deep.equal(["https://www.googleapis.com/auth/drive.readonly"]);
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
      expect(() =>
        createAuthClient({
          projectId: "target-project-id",
          location: "US",
          credentials: "{not-valid-json",
          impersonateServiceAccount: TARGET_SERVICE_ACCOUNT,
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
    // google-auth-library marks scopes private on GoogleAuth.
    expect((defaultClient.authClient as unknown as { scopes: string[] }).scopes).to.deep.equal([
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/drive.readonly",
    ]);
    expect(clientProvider()).to.equal(defaultClient);
    expect(clientProvider("other-project").projectId).to.equal("other-project");
  });

  test("createBigQueryClientProvider shares one impersonated auth client between projects", async () => {
    const sourceClient = new FakeSourceClient();
    const clientProvider = createBigQueryClientProvider(
      dataform.BigQuery.create({
        projectId: "default-project",
        location: "US",
        impersonateServiceAccount: TARGET_SERVICE_ACCOUNT,
      }),
      sourceClient,
    );
    const defaultAuthClient = await clientProvider().authClient.getClient();
    expect(defaultAuthClient).to.be.instanceOf(Impersonated);
    expect((defaultAuthClient as Impersonated).getTargetPrincipal()).to.equal(
      TARGET_SERVICE_ACCOUNT,
    );
    expect(await clientProvider("other-project").authClient.getClient()).to.equal(
      defaultAuthClient,
    );

    await defaultAuthClient.getAccessToken();
    expect(sourceClient.requests).to.have.lengthOf(1);
    expect(sourceClient.requests[0]).to.deep.include({
      method: "POST",
      url: `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${TARGET_SERVICE_ACCOUNT}:generateAccessToken`,
      data: {
        delegates: [],
        scope: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/drive.readonly",
        ],
        lifetime: "3600s",
      },
    });
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
        scope: ["https://www.googleapis.com/auth/cloud-platform"],
        lifetime: "3600s",
      },
    });
  });

  test("impersonated client includes extra scopes in the token exchange request", async () => {
    const sourceClient = new FakeSourceClient();
    const impersonatedClient = createImpersonatedClient(
      TARGET_SERVICE_ACCOUNT,
      new DeferredAuthClient(async () => sourceClient),
      BIGQUERY_EXTRA_SCOPES,
    );

    await impersonatedClient.getAccessToken();

    expect(sourceClient.requests.length).to.equal(1);
    expect(sourceClient.requests[0]).to.deep.include({
      method: "POST",
      url: `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${TARGET_SERVICE_ACCOUNT}:generateAccessToken`,
      data: {
        delegates: [],
        scope: [
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/drive.readonly",
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

  test("table() returns null when table metadata is not found", async () => {
    const mockBigQuery = mock<BigQuery>();
    const mockDataset = mock<Dataset>();
    const mockDeletedTable = mock<Table>();

    when(mockBigQuery.dataset("schema1")).thenReturn(instance(mockDataset));
    when(mockDataset.table("deleted_table")).thenReturn(instance(mockDeletedTable));
    when(mockDeletedTable.getMetadata()).thenReject(
      Object.assign(new Error("Not found"), { errors: [{ reason: "notFound" }] }),
    );

    const adapter = new BigQueryDbAdapter(
      dataform.BigQuery.create({ projectId: "project1", location: "US" }),
      { clientProvider: () => instance(mockBigQuery) },
    );

    expect(
      await adapter.table({ database: "project1", schema: "schema1", name: "deleted_table" }),
    ).to.equal(null);
  });

  test("tables() omits concurrently deleted tables", async () => {
    const mockBigQuery = mock<BigQuery>();
    const mockDataset = mock<Dataset>();
    const mockLiveTable = mock<Table>();
    const mockDeletedTable = mock<Table>();

    when(mockLiveTable.id).thenReturn("live_table");
    when(mockDeletedTable.id).thenReturn("deleted_table");
    when(mockBigQuery.dataset("schema1")).thenReturn(instance(mockDataset));
    when(mockDataset.getTables(anything())).thenResolve([
      [instance(mockLiveTable), instance(mockDeletedTable)],
      {},
      {},
    ]);
    when(mockDataset.table("live_table")).thenReturn(instance(mockLiveTable));
    when(mockDataset.table("deleted_table")).thenReturn(instance(mockDeletedTable));
    when(mockLiveTable.getMetadata()).thenResolve([
      {
        type: "TABLE",
        tableReference: { projectId: "project1", datasetId: "schema1", tableId: "live_table" },
        schema: { fields: [{ name: "col1", type: "STRING" }] },
        lastModifiedTime: "123456789",
      },
      undefined,
    ]);
    when(mockDeletedTable.getMetadata()).thenReject(
      Object.assign(new Error("Not found"), { errors: [{ reason: "notFound" }] }),
    );

    const adapter = new BigQueryDbAdapter(
      dataform.BigQuery.create({ projectId: "project1", location: "US" }),
      { clientProvider: () => instance(mockBigQuery) },
    );

    const tables = await adapter.tables("project1", "schema1");
    expect(tables.map((t) => t.target)).to.deep.equal([
      { database: "project1", schema: "schema1", name: "live_table" },
    ]);
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

    test("leaves out byte counts BigQuery didn't report", async () => {
      for (const jobMetadata of [{}, { statistics: {} }, { statistics: { query: {} } }]) {
        const { adapter } = dryRunAdapter(jobMetadata);

        const result = await adapter.execute("SELECT 1", { bigquery: { dryRun: true } });

        expect(result.metadata.bigquery.totalBytesProcessed).equals(undefined);
        expect(result.metadata.bigquery.referencedTables).deep.equals([]);
        expect(result.metadata.bigquery.schema).deep.equals([]);
        expect(JSON.parse(prettyJsonStringify(result.metadata))).deep.equals({
          bigquery: { referencedTables: [], schema: [] },
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
});
