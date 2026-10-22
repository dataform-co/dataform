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

suite("@dataform/cli/catalog/metadata_writer", () => {
  test("calls updateEntry for single-region dataset with overview and generic aspects", async () => {
    const capturedRequests: IUpdateEntryRequest[] = [];
    const stderrMessages: string[] = [];

    const mockClient: ICatalogClient = {
      updateEntry: (async (req: IUpdateEntryRequest) => {
        capturedRequests.push(req);
        return [{}];
      }) as ICatalogClient["updateEntry"],
    };

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

  test("logs warning and skips updateEntry for multi-region locations (US, EU)", async () => {
    let clientCalled = false;
    const stderrMessages: string[] = [];

    const mockClient: ICatalogClient = {
      updateEntry: (async () => {
        clientCalled = true;
        return [{}];
      }) as ICatalogClient["updateEntry"],
    };

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

    expect(clientCalled).to.equal(false);
    expect(stderrMessages).to.deep.equal([
      "Warning: Skipping Knowledge Catalog metadata write for target-proj.my_dataset.my_table: " +
        'multi-region location "US" is not supported by Knowledge Catalog.\n',
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

  test("propagates updateEntry errors so the action fails loudly", async () => {
    const mockClient: ICatalogClient = {
      updateEntry: (async () => {
        throw new Error("PERMISSION_DENIED: missing dataplex.entries.update");
      }) as ICatalogClient["updateEntry"],
    };

    const writer = new KnowledgeCatalogMetadataWriter(
      dataform.BigQuery.create({ projectId: "default-proj", location: "us-central1" }),
      {
        clientProvider: () => mockClient,
        stderr: { write: () => undefined },
      },
    );

    let thrownError: Error | undefined;
    try {
      await writer.writeMetadata(
        { database: "target-proj", schema: "my_dataset", name: "my_table" },
        "us-central1",
        dataform.Metadata.create({ overview: "Table overview." }),
      );
    } catch (e) {
      thrownError = e as Error;
    }

    expect(thrownError).to.not.equal(undefined);
    expect(thrownError?.message).to.include("PERMISSION_DENIED");
  });

  test("retries transient NOT_FOUND errors while entry is syncing in Knowledge Catalog", async () => {
    let attempts = 0;
    const sleepDelays: number[] = [];

    const mockClient: ICatalogClient = {
      updateEntry: (async () => {
        attempts++;
        if (attempts < 3) {
          const notFoundErr = new Error("5 NOT_FOUND: Entry not found") as Error & { code: number };
          notFoundErr.code = 5;
          throw notFoundErr;
        }
        return [{}];
      }) as ICatalogClient["updateEntry"],
    };

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
});
