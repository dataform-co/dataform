import { expect } from "chai";

import {
  buildBigQueryEntryName,
  buildUpdateEntryRequest,
  extractGenericFields,
  extractOverview,
  GENERIC_ASPECT_KEY,
  GENERIC_ASPECT_TYPE,
  isSupportedCatalogLocation,
  OVERVIEW_ASPECT_KEY,
  OVERVIEW_ASPECT_TYPE,
} from "df/cli/api/catalog/request_builder";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("@dataform/cli/catalog/request_builder", () => {
  suite("isSupportedCatalogLocation", () => {
    test("accepts single-region locations and US/EU multi-regions", () => {
      expect(isSupportedCatalogLocation("us-central1")).to.equal(true);
      expect(isSupportedCatalogLocation("europe-west2")).to.equal(true);
      expect(isSupportedCatalogLocation("US")).to.equal(true);
      expect(isSupportedCatalogLocation("EU")).to.equal(true);
      expect(isSupportedCatalogLocation(" us ")).to.equal(true);
    });

    test("rejects unsupported multi-regions and empty locations", () => {
      expect(isSupportedCatalogLocation("ASIA")).to.equal(false);
      expect(isSupportedCatalogLocation("europe")).to.equal(false);
      expect(isSupportedCatalogLocation("")).to.equal(false);
      expect(isSupportedCatalogLocation("   ")).to.equal(false);
    });
  });

  suite("buildBigQueryEntryName", () => {
    test("builds entry name using target.database and lowercased location", () => {
      const entryName = buildBigQueryEntryName("fallback-proj", "US-CENTRAL1", {
        database: "target-proj",
        schema: "my_dataset",
        name: "my_table",
      });
      expect(entryName).to.equal(
        "projects/target-proj/locations/us-central1/entryGroups/@bigquery/entries/" +
          "bigquery.googleapis.com/projects/target-proj/datasets/my_dataset/tables/my_table",
      );
    });

    test("falls back to projectId when target.database is unset", () => {
      const entryName = buildBigQueryEntryName("fallback-proj", "europe-west1", {
        schema: "my_dataset",
        name: "my_view",
      });
      expect(entryName).to.equal(
        "projects/fallback-proj/locations/europe-west1/entryGroups/@bigquery/entries/" +
          "bigquery.googleapis.com/projects/fallback-proj/datasets/my_dataset/tables/my_view",
      );
    });
  });

  suite("extractOverview", () => {
    test("returns strongly typed overview when extraProperties.overview is absent", () => {
      const metadata = dataform.Metadata.create({
        overview: "Table overview text.",
      });
      expect(extractOverview(metadata)).to.equal("Table overview text.");
    });

    test("returns extraProperties.overview from protobuf Struct", () => {
      const metadata = dataform.Metadata.create({
        extraProperties: {
          fields: {
            overview: { stringValue: "Extra overview text." },
          },
        },
      });
      expect(extractOverview(metadata)).to.equal("Extra overview text.");
    });

    test("joins strongly typed overview and extraProperties.overview with a blank line", () => {
      const metadata = dataform.Metadata.create({
        overview: "Primary overview.",
        extraProperties: {
          fields: {
            overview: { stringValue: "Secondary overview." },
          },
        },
      });
      expect(extractOverview(metadata)).to.equal("Primary overview.\n\nSecondary overview.");
    });

    test("returns null when overview is empty or non-string", () => {
      expect(extractOverview(null)).to.equal(null);
      expect(extractOverview(dataform.Metadata.create({}))).to.equal(null);
      expect(
        extractOverview(
          dataform.Metadata.create({
            extraProperties: {
              fields: {
                overview: { numberValue: 123 },
              },
            },
          }),
        ),
      ).to.equal(null);
    });
  });

  suite("extractGenericFields", () => {
    test("extracts system and type from protobuf Struct and ignores unsupported keys", () => {
      const metadata = dataform.Metadata.create({
        extraProperties: {
          fields: {
            generic: {
              structValue: {
                fields: {
                  system: { stringValue: "BigQuery" },
                  type: { stringValue: "table" },
                  unsupportedKey: { stringValue: "ignored" },
                },
              },
            },
          },
        },
      });
      expect(extractGenericFields(metadata)).to.deep.equal({
        system: { stringValue: "BigQuery" },
        type: { stringValue: "table" },
      });
    });

    test("extracts system and type from plain object extraProperties", () => {
      const metadata: dataform.IMetadata = {
        extraProperties: {
          generic: {
            system: "Dataform",
            type: "view",
            other: 42,
          },
        } as unknown as dataform.IMetadata["extraProperties"],
      };
      expect(extractGenericFields(metadata)).to.deep.equal({
        system: { stringValue: "Dataform" },
        type: { stringValue: "view" },
      });
    });

    test("returns null when generic has no supported string fields", () => {
      const metadata = dataform.Metadata.create({
        extraProperties: {
          fields: {
            generic: {
              structValue: {
                fields: {
                  unsupportedKey: { stringValue: "ignored" },
                  system: { numberValue: 42 },
                },
              },
            },
          },
        },
      });
      expect(extractGenericFields(metadata)).to.equal(null);
    });
  });

  suite("buildUpdateEntryRequest", () => {
    test("builds UpdateEntryRequest with overview and generic aspects", () => {
      const metadata = dataform.Metadata.create({
        overview: "Standardized trip data.",
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
      });

      const req = buildUpdateEntryRequest(
        "my-proj",
        "us-central1",
        { database: "my-proj", schema: "analytics", name: "trips" },
        metadata,
      );

      expect(req).to.deep.equal({
        entry: {
          name:
            "projects/my-proj/locations/us-central1/entryGroups/@bigquery/entries/" +
            "bigquery.googleapis.com/projects/my-proj/datasets/analytics/tables/trips",
          aspects: {
            [OVERVIEW_ASPECT_KEY]: {
              aspectType: OVERVIEW_ASPECT_TYPE,
              data: {
                fields: {
                  content: { stringValue: "Standardized trip data." },
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
      });
    });

    test("returns null when metadata contains no supported aspects", () => {
      const metadata = dataform.Metadata.create({
        extraProperties: {
          fields: {
            customKey: { stringValue: "ignored" },
          },
        },
      });

      const req = buildUpdateEntryRequest(
        "my-proj",
        "us-central1",
        { database: "my-proj", schema: "analytics", name: "trips" },
        metadata,
      );
      expect(req).to.equal(null);
    });
  });
});
