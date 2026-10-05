import { Dataset, Table } from "@google-cloud/bigquery";
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";
import { anything, instance, mock, when } from "ts-mockito";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter, createBigQueryClientProvider } from "df/cli/api/dbadapters/bigquery";
import {
  bridgeCloudSdkConfig,
  ensureAdcAvailable,
  resolveCredentialsPath,
} from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

interface IBigQueryClientInternal {
  projectId: string;
  location: string;
  authClient: {
    jsonContent: unknown;
  };
}

suite("BigQueryDbAdapter", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("createBigQueryClientProvider parses service account key from --credentials file", async () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const credentialsFilePath = path.join(tmpDir, ".df-credentials.json");
    const serviceAccountKey = {
      type: "service_account",
      project_id: "key-project-id",
      private_key_id: "key-id-123",
      private_key:
        "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC\n-----END PRIVATE KEY-----\n",
      client_email: "test-sa@key-project-id.iam.gserviceaccount.com",
      client_id: "1234567890",
    };
    fs.writeFileSync(
      credentialsFilePath,
      JSON.stringify({
        projectId: "target-project-id",
        location: "EU",
        credentials: JSON.stringify(serviceAccountKey),
      }),
    );

    const readCredentials = dfapi.credentials.read(credentialsFilePath);
    expect(readCredentials.projectId).to.equal("target-project-id");
    expect(readCredentials.location).to.equal("EU");
    expect(readCredentials.credentials).to.equal(JSON.stringify(serviceAccountKey));

    const clientProvider = createBigQueryClientProvider(readCredentials);
    const client = clientProvider() as unknown as IBigQueryClientInternal;
    expect(client.projectId).to.equal("target-project-id");
    expect(client.location).to.equal("EU");
    expect(client.authClient.jsonContent).to.deep.equal(serviceAccountKey);

    // Also verify BigQueryDbAdapter works with key-file credentials and a mocked client.
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();
    when(mockBigQuery.dataset("schema1")).thenReturn(instance(mockDataset));
    when(mockDataset.getTables(anything())).thenReturn(Promise.resolve([[]] as any));

    const adapter = new BigQueryDbAdapter(readCredentials, {
      clientProvider: () => instance(mockBigQuery),
    });
    const tables = await adapter.tables("target-project-id", "schema1");
    expect(tables).to.eql([]);
  });

  test("createBigQueryClientProvider uses Application Default Credentials when key is omitted", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const credentialsFilePath = path.join(tmpDir, "bigquery.json");
    fs.writeFileSync(
      credentialsFilePath,
      JSON.stringify({
        projectId: "adc-project-id",
        location: "US",
      }),
    );

    const readCredentials = dfapi.credentials.read(credentialsFilePath);
    expect(readCredentials.projectId).to.equal("adc-project-id");
    expect(readCredentials.location).to.equal("US");
    expect(readCredentials.credentials).to.equal("");

    const clientProvider = createBigQueryClientProvider(readCredentials);
    const client = clientProvider() as unknown as IBigQueryClientInternal;
    expect(client.projectId).to.equal("adc-project-id");
    expect(client.location).to.equal("US");
    expect(client.authClient.jsonContent).to.equal(null);
  });

  test("createBigQueryClientProvider throws on malformed service account key JSON", () => {
    const clientProvider = createBigQueryClientProvider({
      projectId: "target-project-id",
      location: "US",
      credentials: "{not-valid-json",
    });
    expect(() => clientProvider()).to.throw(SyntaxError);
  });

  test("resolveCredentialsPath respects priority order", () => {
    const baseDir = tmpDirFixture.createNewTmpDir();
    const credsDir = path.join(baseDir, "test_credentials");
    fs.mkdirpSync(credsDir);

    const defaultFile = path.join(credsDir, "bigquery.json");
    const localFile = path.join(credsDir, "bigquery.local.json");
    const customFile = path.join(baseDir, "custom_credentials.json");
    fs.writeFileSync(defaultFile, "{}");

    // 1. Default when neither local file nor env var is present.
    expect(resolveCredentialsPath({}, baseDir)).to.equal(defaultFile);

    // 2. Local override takes precedence over default.
    fs.writeFileSync(localFile, "{}");
    expect(resolveCredentialsPath({}, baseDir)).to.equal(localFile);

    // 3. DATAFORM_TEST_CREDENTIALS takes precedence over both.
    fs.writeFileSync(customFile, "{}");
    expect(resolveCredentialsPath({ DATAFORM_TEST_CREDENTIALS: customFile }, baseDir)).to.equal(
      customFile,
    );
    expect(
      resolveCredentialsPath({ DATAFORM_TEST_CREDENTIALS: "custom_credentials.json" }, baseDir),
    ).to.equal(customFile);
  });

  test("bridgeCloudSdkConfig populates GOOGLE_APPLICATION_CREDENTIALS when unset", () => {
    const cloudsdkDir = tmpDirFixture.createNewTmpDir();
    const adcFile = path.join(cloudsdkDir, "application_default_credentials.json");
    fs.writeFileSync(adcFile, "{}");

    const envWithoutGoogleCreds: NodeJS.ProcessEnv = { CLOUDSDK_CONFIG: cloudsdkDir };
    bridgeCloudSdkConfig(envWithoutGoogleCreds);
    expect(envWithoutGoogleCreds.GOOGLE_APPLICATION_CREDENTIALS).to.equal(adcFile);

    const envWithExistingCreds: NodeJS.ProcessEnv = {
      CLOUDSDK_CONFIG: cloudsdkDir,
      GOOGLE_APPLICATION_CREDENTIALS: "/existing/creds.json",
    };
    bridgeCloudSdkConfig(envWithExistingCreds);
    expect(envWithExistingCreds.GOOGLE_APPLICATION_CREDENTIALS).to.equal("/existing/creds.json");
  });

  test("ensureAdcAvailable succeeds for explicit key, env var, or gcloud ADC and fails fast when missing", async () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const adcCredentialsFile = path.join(tmpDir, "bigquery.json");
    fs.writeFileSync(
      adcCredentialsFile,
      JSON.stringify({ projectId: "test-project", location: "US" }),
    );

    const keyCredentialsFile = path.join(tmpDir, "bigquery_with_key.json");
    fs.writeFileSync(
      keyCredentialsFile,
      JSON.stringify({
        projectId: "test-project",
        location: "US",
        credentials: '{"type":"service_account"}',
      }),
    );

    // 1. Explicit service account key in Dataform credentials file succeeds without ADC.
    await ensureAdcAvailable({
      credentialsPath: keyCredentialsFile,
      env: { METADATA_SERVER_DETECTION: "none" },
    });

    // 2. Valid GOOGLE_APPLICATION_CREDENTIALS file succeeds.
    const validAdcPath = path.join(tmpDir, "adc.json");
    fs.writeFileSync(validAdcPath, "{}");
    await ensureAdcAvailable({
      credentialsPath: adcCredentialsFile,
      env: { GOOGLE_APPLICATION_CREDENTIALS: validAdcPath, METADATA_SERVER_DETECTION: "none" },
    });

    // 3. Non-existent GOOGLE_APPLICATION_CREDENTIALS fails immediately with clear message.
    let missingEnvError: Error | undefined;
    try {
      await ensureAdcAvailable({
        credentialsPath: adcCredentialsFile,
        env: {
          GOOGLE_APPLICATION_CREDENTIALS: path.join(tmpDir, "does_not_exist.json"),
          METADATA_SERVER_DETECTION: "none",
        },
      });
    } catch (e) {
      missingEnvError = e as Error;
    }
    expect(missingEnvError?.message).to.include(
      "GOOGLE_APPLICATION_CREDENTIALS points to a non-existent file",
    );

    // 4. HOME/.config/gcloud/application_default_credentials.json succeeds.
    const gcloudDir = path.join(tmpDir, ".config", "gcloud");
    fs.mkdirpSync(gcloudDir);
    fs.writeFileSync(path.join(gcloudDir, "application_default_credentials.json"), "{}");
    await ensureAdcAvailable({
      credentialsPath: adcCredentialsFile,
      env: { HOME: tmpDir, METADATA_SERVER_DETECTION: "none" },
      platform: "linux",
    });

    // 5. Missing ADC everywhere fails immediately with actionable instructions.
    const emptyHome = tmpDirFixture.createNewTmpDir();
    let noAdcError: Error | undefined;
    try {
      await ensureAdcAvailable({
        credentialsPath: adcCredentialsFile,
        env: { HOME: emptyHome, METADATA_SERVER_DETECTION: "none" },
        platform: "linux",
      });
    } catch (e) {
      noAdcError = e as Error;
    }
    expect(noAdcError?.message).to.equal(
      "No Application Default Credentials found. Run `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS.",
    );
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
});
