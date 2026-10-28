import { expect } from "chai";

import { ICatalogClient, KnowledgeCatalogMetadataWriter } from "df/cli/api/catalog/metadata_writer";
import {
  GENERIC_ASPECT_KEY,
  GENERIC_ASPECT_TYPE,
  IUpdateEntryRequest,
  OVERVIEW_ASPECT_KEY,
  OVERVIEW_ASPECT_TYPE,
} from "df/cli/api/catalog/request_builder";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

function createMockCatalogClient(
  overrides: Partial<ICatalogClient> & Pick<ICatalogClient, "updateEntry">,
): ICatalogClient {
  return {
    getEntry: (async () => [{}]) as ICatalogClient["getEntry"],
    ...overrides,
  };
}

suite("@dataform/cli/catalog/metadata_writer", () => {
  test("calls updateEntry for single-region dataset with overview and generic aspects", async () => {
    const capturedRequests: IUpdateEntryRequest[] = [];
    const stderrMessages: string[] = [];

    const mockClient = createMockCatalogClient({
      updateEntry: (async (req: IUpdateEntryRequest) => {
        capturedRequests.push(req);
        return [{}];
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "us-central1",
      dataform.Metadata.create({
        overview: "Table overview.",
        extraProperties: {
          fields: {
            generic: {
              structValue: {
                fields: {
                  system: { stringValue: "BigQuery" },
                  type: { stringValue: "table" },
                },
              },
            },
          },
        },
      }),
    );

    expect(stderrMessages).to.deep.equal([]);
    expect(capturedRequests).to.deep.equal([
      {
        entry: {
          name:
            "projects/target-proj/locations/us-central1/entryGroups/@bigquery/entries/" +
            "bigquery.googleapis.com/projects/target-proj/datasets/my_dataset/tables/my_table",
          aspects: {
            [OVERVIEW_ASPECT_KEY]: {
              aspectType: OVERVIEW_ASPECT_TYPE,
              data: {
                fields: {
                  content: { stringValue: "Table overview." },
                },
              },
            },
            [GENERIC_ASPECT_KEY]: {
              aspectType: GENERIC_ASPECT_TYPE,
              data: {
                fields: {
                  system: { stringValue: "BigQuery" },
                  type: { stringValue: "table" },
                },
              },
            },
          },
        },
        updateMask: {
          paths: ["aspects"],
        },
      },
    ]);
  });

  test("calls updateEntry for US/EU multi-region locations using lowercased location", async () => {
    const capturedRequests: IUpdateEntryRequest[] = [];
    const stderrMessages: string[] = [];

    const mockClient = createMockCatalogClient({
      updateEntry: (async (req: IUpdateEntryRequest) => {
        capturedRequests.push(req);
        return [{}];
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "US" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "US",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(stderrMessages).to.deep.equal([]);
    expect(capturedRequests[0]?.entry?.name).to.equal(
      "projects/target-proj/locations/us/entryGroups/@bigquery/entries/" +
        "bigquery.googleapis.com/projects/target-proj/datasets/my_dataset/tables/my_table",
    );
  });

  test("logs warning and skips updateEntry for unsupported locations (e.g. ASIA)", async () => {
    let clientCalled = false;
    const stderrMessages: string[] = [];

    const mockClient = createMockCatalogClient({
      updateEntry: (async () => {
        clientCalled = true;
        return [{}];
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "ASIA" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "ASIA",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(clientCalled).to.equal(false);
    expect(stderrMessages).to.deep.equal([
      "Warning: Skipping Knowledge Catalog metadata write for target-proj.my_dataset.my_table: " +
        'location "ASIA" is not supported by Knowledge Catalog.\n',
    ]);
  });

  test("does not instantiate client or log warning when metadata has no supported aspects", async () => {
    let clientProviderCalled = false;
    const stderrMessages: string[] = [];

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "US" }),
      {
        clientProvider: () => {
          clientProviderCalled = true;
          throw new Error("Should not be called");
        },
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "US",
      dataform.Metadata.create({}),
    );

    expect(clientProviderCalled).to.equal(false);
    expect(stderrMessages).to.deep.equal([]);
  });

  test("skips remaining writes per project on genuine PERMISSION_DENIED while allowing other projects", async () => {
    const attemptedProjects: string[] = [];
    const stderrMessages: string[] = [];

    const mockClient: ICatalogClient = {
      updateEntry: (async (req: IUpdateEntryRequest) => {
        const entryName = req.entry?.name || "";
        const match = /^projects\/([^/]+)\//.exec(entryName);
        const project = match?.[1] || "";
        attemptedProjects.push(project);
        if (project === "denied-proj") {
          const err = new Error(
            "7 PERMISSION_DENIED: Permission 'dataplex.entries.update' denied on resource (or it may not exist).",
          ) as Error & { code: number };
          err.code = 7;
          throw err;
        }
        return [{}];
      }) as ICatalogClient["updateEntry"],
      getEntry: (async () => {
        const err = new Error("7 PERMISSION_DENIED: missing dataplex.entries.get") as Error & {
          code: number;
        };
        err.code = 7;
        throw err;
      }) as ICatalogClient["getEntry"],
    };

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "denied-proj", schema: "my_dataset", name: "table_1" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table 1 overview." }),
    );
    await writer.writeMetadata(
      { database: "denied-proj", schema: "my_dataset", name: "table_2" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table 2 overview." }),
    );
    await writer.writeMetadata(
      { database: "allowed-proj", schema: "my_dataset", name: "table_3" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table 3 overview." }),
    );

    expect(attemptedProjects).to.deep.equal(["denied-proj", "allowed-proj"]);
    expect(stderrMessages.length).to.equal(1);
    expect(stderrMessages[0]).to.include(
      "Warning: Skipping Knowledge Catalog metadata writes for project denied-proj for the rest of this run (failed on denied-proj.my_dataset.table_1): 7 PERMISSION_DENIED",
    );
  });

  test("retries transient PERMISSION_DENIED when getEntry reports NOT_FOUND", async () => {
    let attempts = 0;
    const sleepDelays: number[] = [];

    const mockClient: ICatalogClient = {
      updateEntry: (async () => {
        attempts++;
        if (attempts === 1) {
          const err = new Error(
            "7 PERMISSION_DENIED: Permission denied on resource (or it may not exist).",
          ) as Error & { code: number };
          err.code = 7;
          throw err;
        }
        return [{}];
      }) as ICatalogClient["updateEntry"],
      getEntry: (async () => {
        const err = new Error("5 NOT_FOUND: Entry not found") as Error & { code: number };
        err.code = 5;
        throw err;
      }) as ICatalogClient["getEntry"],
    };

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "EU" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: () => undefined },
        sleepFn: async (millis: number) => {
          sleepDelays.push(millis);
        },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "EU",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(attempts).to.equal(2);
    expect(sleepDelays).to.deep.equal([1000]);
  });

  test("logs warning and fails open when NOT_FOUND retries are exhausted", async () => {
    let attempts = 0;
    const stderrMessages: string[] = [];

    const mockClient = createMockCatalogClient({
      updateEntry: (async () => {
        attempts++;
        const notFoundErr = new Error("5 NOT_FOUND: Entry not found") as Error & { code: number };
        notFoundErr.code = 5;
        throw notFoundErr;
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
        sleepFn: async () => undefined,
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(attempts).to.equal(5);
    expect(stderrMessages).to.deep.equal([
      "Warning: Skipping Knowledge Catalog metadata write for target-proj.my_dataset.my_table: " +
        "entry was not found in Knowledge Catalog after 5 attempts.\n",
    ]);
  });

  test("logs warning and does not throw on unexpected updateEntry errors", async () => {
    const stderrMessages: string[] = [];
    const mockClient = createMockCatalogClient({
      updateEntry: (async () => {
        const err = new Error("3 INVALID_ARGUMENT: malformed aspect") as Error & { code: number };
        err.code = 3;
        throw err;
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: (msg: string) => stderrMessages.push(msg) },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(stderrMessages).to.deep.equal([
      "Warning: Failed to write Knowledge Catalog metadata for target-proj.my_dataset.my_table: " +
        "3 INVALID_ARGUMENT: malformed aspect\n",
    ]);
  });

  test("retries transient NOT_FOUND errors while entry is syncing in Knowledge Catalog", async () => {
    let attempts = 0;
    const sleepDelays: number[] = [];

    const mockClient = createMockCatalogClient({
      updateEntry: (async () => {
        attempts++;
        if (attempts < 3) {
          const notFoundErr = new Error("5 NOT_FOUND: Entry not found") as Error & { code: number };
          notFoundErr.code = 5;
          throw notFoundErr;
        }
        return [{}];
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: () => undefined },
        sleepFn: async (millis: number) => {
          sleepDelays.push(millis);
        },
      },
    );

    await writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table overview." }),
    );

    expect(attempts).to.equal(3);
    expect(sleepDelays).to.deep.equal([1000, 2000]);
  });

  test("drain waits for all pending writes to complete", async () => {
    let finished = false;
    const mockClient = createMockCatalogClient({
      updateEntry: (async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        finished = true;
        return [{}];
      }) as ICatalogClient["updateEntry"],
    });

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: () => undefined },
      },
    );

    void writer.writeMetadata(
      { database: "target-proj", schema: "my_dataset", name: "my_table" },
      "us-central1",
      dataform.Metadata.create({ overview: "Table overview." }),
    );
    expect(finished).to.equal(false);

    await writer.drain();
    expect(finished).to.equal(true);
  });
});
