import { CatalogServiceClient } from "@google-cloud/dataplex";

import { toProtoStruct } from "df/cli/api/lineage/payload_builder";
import { Structs } from "df/common/protos/structs";
import { dataform, google } from "df/protos/ts";

export type IUpdateEntryRequest = NonNullable<Parameters<CatalogServiceClient["updateEntry"]>[0]>;

export interface IAspectPayload {
  aspectType: string;
  data: {
    fields: {
      [key: string]: {
        stringValue: string;
      };
    };
  };
}

export const OVERVIEW_ASPECT_KEY = "dataplex-types.global.overview";
export const OVERVIEW_ASPECT_TYPE = "projects/dataplex-types/locations/global/aspectTypes/overview";
export const GENERIC_ASPECT_KEY = "dataplex-types.global.generic";
export const GENERIC_ASPECT_TYPE = "projects/dataplex-types/locations/global/aspectTypes/generic";

const SUPPORTED_GENERIC_KEYS = new Set(["system", "type"]);
const SUPPORTED_MULTI_REGIONS = new Set(["us", "eu"]);

/**
 * Returns true if `location` is supported by Knowledge Catalog:
 * any single-region location (contains `-`, e.g. `us-central1`) or the `US` / `EU` multi-regions.
 */
export function isSupportedCatalogLocation(location: string): boolean {
  const normalized = location.trim().toLowerCase();
  return (
    normalized.length > 0 && (normalized.includes("-") || SUPPORTED_MULTI_REGIONS.has(normalized))
  );
}

/**
 * Builds the Knowledge Catalog (Dataplex) resource name for a BigQuery table or view entry.
 */
export function buildBigQueryEntryName(
  projectId: string,
  location: string,
  target: dataform.ITarget,
): string {
  const targetProject = target.database || projectId;
  const normalizedLocation = location.trim().toLowerCase();
  return (
    `projects/${targetProject}/locations/${normalizedLocation}` +
    `/entryGroups/@bigquery/entries/bigquery.googleapis.com` +
    `/projects/${targetProject}/datasets/${target.schema}/tables/${target.name}`
  );
}

/**
 * Extracts the effective `overview` text from `metadata.overview` and
 * `metadata.extraProperties.overview`. When both are present, joins them with a blank line.
 */
export function extractOverview(metadata: dataform.IMetadata | null | undefined): string | null {
  if (!metadata) {
    return null;
  }
  const extraOverview = getExtraPropertiesObject(metadata.extraProperties)?.overview;
  const parts = [metadata.overview, extraOverview].filter(
    (v): v is string => typeof v === "string" && v.length > 0,
  );
  return parts.length > 0 ? parts.join("\n\n") : null;
}

/**
 * Extracts supported `system` and `type` string fields from `metadata.extraProperties.generic`.
 */
export function extractGenericFields(
  metadata: dataform.IMetadata | null | undefined,
): { [key: string]: { stringValue: string } } | null {
  const generic = getExtraPropertiesObject(metadata?.extraProperties)?.generic;
  if (!generic || typeof generic !== "object" || Array.isArray(generic)) {
    return null;
  }

  const filtered: { [key: string]: string } = {};
  for (const [key, value] of Object.entries(generic)) {
    if (SUPPORTED_GENERIC_KEYS.has(key) && typeof value === "string") {
      filtered[key] = value;
    }
  }

  return Object.keys(filtered).length > 0 ? toProtoStruct(filtered).fields : null;
}

/**
 * Builds the Dataplex `aspects` map for `overview` and `extraProperties.generic`.
 * Returns `null` if no supported catalog metadata is present.
 */
export function buildCatalogAspects(
  metadata: dataform.IMetadata | null | undefined,
): { [key: string]: IAspectPayload } | null {
  const aspects: { [key: string]: IAspectPayload } = {};

  const overview = extractOverview(metadata);
  if (overview !== null) {
    aspects[OVERVIEW_ASPECT_KEY] = {
      aspectType: OVERVIEW_ASPECT_TYPE,
      data: toProtoStruct({ content: overview }),
    };
  }

  const genericFields = extractGenericFields(metadata);
  if (genericFields !== null) {
    aspects[GENERIC_ASPECT_KEY] = {
      aspectType: GENERIC_ASPECT_TYPE,
      data: {
        fields: genericFields,
      },
    };
  }

  if (Object.keys(aspects).length === 0) {
    return null;
  }
  return aspects;
}

/**
 * Builds a Dataplex `UpdateEntryRequest` for the given BigQuery target and action metadata,
 * or returns `null` if there are no supported aspects to write.
 */
export function buildUpdateEntryRequest(
  projectId: string,
  location: string,
  target: dataform.ITarget,
  metadata: dataform.IMetadata | null | undefined,
): IUpdateEntryRequest | null {
  const aspects = buildCatalogAspects(metadata);
  if (!aspects) {
    return null;
  }

  return {
    entry: {
      name: buildBigQueryEntryName(projectId, location, target),
      aspects,
    },
    updateMask: {
      paths: ["aspects"],
    },
  };
}

function getExtraPropertiesObject(
  extraProperties: google.protobuf.IStruct | { [key: string]: unknown } | null | undefined,
): { [key: string]: unknown } | undefined {
  if (!extraProperties || typeof extraProperties !== "object") {
    return undefined;
  }
  return (
    Structs.toObject(extraProperties as google.protobuf.IStruct) ??
    (extraProperties as { [key: string]: unknown })
  );
}
