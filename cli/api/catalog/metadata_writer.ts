import { CatalogServiceClient } from "@google-cloud/dataplex";
import { AuthClient } from "google-auth-library";

import {
  buildCatalogAspects,
  buildUpdateEntryRequest,
  isSupportedCatalogLocation,
} from "df/cli/api/catalog/request_builder";
import type { IStderrLike } from "df/cli/api/lineage/emitter";
import { coerceAsError } from "df/common/errors/errors";
import { sleep } from "df/common/promises";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

export type ICatalogClient = Pick<CatalogServiceClient, "updateEntry"> &
  Partial<Pick<CatalogServiceClient, "getEntry">>;
export type CatalogClientProvider = (projectId?: string) => ICatalogClient;

const DATAFORM_CLI_LIB_NAME = "dataform-cli";
const NOT_FOUND_MAX_ATTEMPTS = 5;
const NOT_FOUND_INITIAL_DELAY_MILLIS = 1000;

export const CATALOG_RETRY_CONFIG = {
  retryCodes: [4, 8, 13, 14], // DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL, UNAVAILABLE
  backoffSettings: {
    initialRetryDelayMillis: 100,
    retryDelayMultiplier: 1.3,
    maxRetryDelayMillis: 5000,
    initialRpcTimeoutMillis: 5000,
    rpcTimeoutMultiplier: 1.0,
    maxRpcTimeoutMillis: 5000,
    totalTimeoutMillis: 15000,
  },
};

function isNotFoundError(err: unknown): boolean {
  if (!err || typeof err !== "object") {
    return false;
  }
  const code = (err as { code?: unknown }).code;
  const message = (err as { message?: unknown }).message;
  return code === 5 || (typeof message === "string" && message.includes("NOT_FOUND"));
}

function isPermissionDeniedError(err: unknown): boolean {
  if (!err || typeof err !== "object") {
    return false;
  }
  const code = (err as { code?: unknown }).code;
  const message = (err as { message?: unknown }).message;
  return code === 7 || (typeof message === "string" && message.includes("PERMISSION_DENIED"));
}

export function createCatalogClientProvider(
  credentials: dataform.IBigQuery,
  authClient?: AuthClient,
): (projectId?: string) => CatalogServiceClient {
  const clients = new Map<string, CatalogServiceClient>();
  return (projectId?: string) => {
    const targetProjectId = projectId || credentials.projectId;
    if (!clients.has(targetProjectId)) {
      clients.set(
        targetProjectId,
        new CatalogServiceClient({
          projectId: targetProjectId,
          ...(authClient
            ? { authClient }
            : { credentials: credentials.credentials && JSON.parse(credentials.credentials) }),
          libName: DATAFORM_CLI_LIB_NAME,
          libVersion: version,
        }),
      );
    }
    return clients.get(targetProjectId);
  };
}

export class KnowledgeCatalogMetadataWriter {
  private readonly credentials: dataform.IBigQuery;
  private readonly clientProvider: CatalogClientProvider;
  private readonly stderr: IStderrLike;
  private readonly sleepFn: (millis: number) => Promise<void>;
  private writesDisabledThisRun = false;

  constructor(
    credentials: dataform.IBigQuery,
    options?: {
      authClient?: AuthClient;
      clientProvider?: CatalogClientProvider;
      stderr?: IStderrLike;
      sleepFn?: (millis: number) => Promise<void>;
    },
  ) {
    this.credentials = credentials;
    this.clientProvider =
      options?.clientProvider || createCatalogClientProvider(credentials, options?.authClient);
    this.stderr = options?.stderr || process.stderr;
    this.sleepFn = options?.sleepFn || sleep;
  }

  public async writeMetadata(
    target: dataform.ITarget,
    location: string | undefined,
    metadata: dataform.IMetadata | null | undefined,
  ): Promise<void> {
    if (this.writesDisabledThisRun) {
      return;
    }

    if (!buildCatalogAspects(metadata)) {
      return;
    }

    const targetProject = target.database || this.credentials.projectId;
    const targetFqn = `${targetProject}.${target.schema}.${target.name}`;
    const trimmedLocation = location?.trim() || "";

    if (!isSupportedCatalogLocation(trimmedLocation)) {
      this.stderr.write(
        trimmedLocation
          ? `Warning: Skipping Knowledge Catalog metadata write for ${targetFqn}: ` +
              `location "${trimmedLocation}" is not supported by Knowledge Catalog.\n`
          : `Warning: Skipping Knowledge Catalog metadata write for ${targetFqn}: ` +
              `dataset location is unknown.\n`,
      );
      return;
    }

    const request = buildUpdateEntryRequest(targetProject, trimmedLocation, target, metadata);
    if (!request) {
      return;
    }

    const client = this.clientProvider(targetProject);
    let delayMillis = NOT_FOUND_INITIAL_DELAY_MILLIS;
    for (let attempt = 1; attempt <= NOT_FOUND_MAX_ATTEMPTS; attempt++) {
      try {
        await client.updateEntry(request, { retry: CATALOG_RETRY_CONFIG });
        return;
      } catch (e) {
        let entryNotFound = isNotFoundError(e);
        // When a BigQuery table is newly created, UpdateEntry may return PERMISSION_DENIED
        // before the entry is synced in Knowledge Catalog. Check GetEntry to distinguish
        // transient sync delay (GetEntry returns NOT_FOUND) from genuine IAM denial.
        if (!entryNotFound && isPermissionDeniedError(e) && client.getEntry) {
          try {
            await client.getEntry(
              { name: request.entry.name, view: "BASIC" },
              { retry: CATALOG_RETRY_CONFIG },
            );
          } catch (getErr) {
            entryNotFound = isNotFoundError(getErr);
          }
        }

        if (entryNotFound) {
          if (attempt < NOT_FOUND_MAX_ATTEMPTS) {
            await this.sleepFn(delayMillis);
            delayMillis *= 2;
            continue;
          }
          this.stderr.write(
            `Warning: Skipping Knowledge Catalog metadata write for ${targetFqn}: ` +
              `entry was not found in Knowledge Catalog after ${NOT_FOUND_MAX_ATTEMPTS} attempts.\n`,
          );
          return;
        }
        if (this.handleNonFatalAccessError(coerceAsError(e), targetProject, targetFqn)) {
          return;
        }
        throw e;
      }
    }
  }

  private handleNonFatalAccessError(err: Error, projectId: string, targetFqn: string): boolean {
    const code = (err as { code?: unknown }).code;
    const message = err.message || "";

    let reason: string | undefined;
    if (code === 7 || message.includes("PERMISSION_DENIED")) {
      reason =
        `PERMISSION_DENIED (${message}). Ensure the credential has 'roles/dataplex.catalogEditor' ` +
        `or that the Dataplex API is enabled in project ${projectId}.`;
    } else if (
      code === 9 ||
      message.includes("SERVICE_DISABLED") ||
      message.includes("FAILED_PRECONDITION")
    ) {
      reason = `Dataplex API is not enabled in project ${projectId} (${message}).`;
    } else if (code === 16 || message.includes("UNAUTHENTICATED")) {
      reason = `UNAUTHENTICATED (${message}).`;
    }

    if (!reason) {
      return false;
    }
    if (!this.writesDisabledThisRun) {
      this.writesDisabledThisRun = true;
      this.stderr.write(
        `Warning: Skipping Knowledge Catalog metadata writes for the rest of this run ` +
          `(failed on ${targetFqn}): ${reason}\n`,
      );
    }
    return true;
  }
}
