import { CatalogServiceClient } from "@google-cloud/dataplex";

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

/**
 * Returns true if `location` is a BigQuery multi-region (e.g. `US`, `EU`).
 * Single-region locations always contain a hyphen (e.g. `us-central1`, `europe-west1`).
 */
export function isMultiRegionLocation(location: string): boolean {
  const normalized = location.trim();
  return normalized.length > 0 && !normalized.includes("-");
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

  const typedOverview =
    typeof metadata.overview === "string" && metadata.overview.length > 0
      ? metadata.overview
      : null;

  const extraOverviewRaw = getExtraPropertyField(metadata.extraProperties, "overview");
  const extraOverview =
    typeof extraOverviewRaw === "string" && extraOverviewRaw.length > 0 ? extraOverviewRaw : null;

  if (typedOverview !== null && extraOverview !== null) {
    return `${typedOverview}\n\n${extraOverview}`;
  }
  if (typedOverview !== null) {
    return typedOverview;
  }
  if (extraOverview !== null) {
    return extraOverview;
  }
  return null;
}

/**
 * Extracts supported `system` and `type` string fields from `metadata.extraProperties.generic`.
 */
export function extractGenericFields(
  metadata: dataform.IMetadata | null | undefined,
): { [key: string]: { stringValue: string } } | null {
  if (!metadata?.extraProperties) {
    return null;
  }

  const genericRaw = getExtraPropertyField(metadata.extraProperties, "generic");
  if (!genericRaw || typeof genericRaw !== "object" || Array.isArray(genericRaw)) {
    return null;
  }

  const genericEntries = extractStructEntries(genericRaw);
  const fields: { [key: string]: { stringValue: string } } = {};

  for (const [key, value] of Object.entries(genericEntries)) {
    if (SUPPORTED_GENERIC_KEYS.has(key) && typeof value === "string") {
      fields[key] = { stringValue: value };
    }
  }

  if (Object.keys(fields).length === 0) {
    return null;
  }
  return fields;
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
      data: {
        fields: {
          content: { stringValue: overview },
        },
      },
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

function getExtraPropertyField(
  extraProperties: google.protobuf.IStruct | { [key: string]: unknown } | null | undefined,
  key: string,
): unknown {
  if (!extraProperties || typeof extraProperties !== "object") {
    return undefined;
  }
  const entries = extractStructEntries(extraProperties);
  return entries[key];
}

function extractStructEntries(
  structOrObject: google.protobuf.IStruct | { [key: string]: unknown },
): { [key: string]: unknown } {
  if (
    "fields" in structOrObject &&
    structOrObject.fields &&
    typeof structOrObject.fields === "object" &&
    !isProtoValueObject(structOrObject.fields)
  ) {
    const result: { [key: string]: unknown } = {};
    for (const [k, v] of Object.entries(structOrObject.fields)) {
      result[k] = unwrapProtoValue(v);
    }
    return result;
  }
  return structOrObject as { [key: string]: unknown };
}

function isProtoValueObject(obj: unknown): boolean {
  if (!obj || typeof obj !== "object") {
    return false;
  }
  return (
    "stringValue" in obj ||
    "numberValue" in obj ||
    "boolValue" in obj ||
    "structValue" in obj ||
    "listValue" in obj ||
    "nullValue" in obj
  );
}

function unwrapProtoValue(val: unknown): unknown {
  if (!val || typeof val !== "object") {
    return val;
  }
  const protoVal = val as google.protobuf.IValue;
  if (typeof protoVal.stringValue === "string") {
    return protoVal.stringValue;
  }
  if (typeof protoVal.numberValue === "number") {
    return protoVal.numberValue;
  }
  if (typeof protoVal.boolValue === "boolean") {
    return protoVal.boolValue;
  }
  if (protoVal.structValue && typeof protoVal.structValue === "object") {
    return extractStructEntries(protoVal.structValue);
  }
  if (protoVal.listValue && Array.isArray(protoVal.listValue.values)) {
    return protoVal.listValue.values.map(unwrapProtoValue);
  }
  if ("nullValue" in protoVal) {
    return null;
  }
  return val;
}
