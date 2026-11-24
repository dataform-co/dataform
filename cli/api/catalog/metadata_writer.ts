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

export type ICatalogClient = Pick<CatalogServiceClient, "updateEntry" | "getEntry">;
export type CatalogClientProvider = () => ICatalogClient;

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
  return Boolean(err && typeof err === "object" && (err as { code?: unknown }).code === 5);
}

function isPermissionDeniedError(err: unknown): boolean {
  return Boolean(err && typeof err === "object" && (err as { code?: unknown }).code === 7);
}

/**
 * Returns the authentication options for Google Cloud clients: uses `authClient` when impersonating
 * a service account, or parses the service account key JSON in `credentials.credentials` (falling
 * back to Application Default Credentials when empty).
 */
export function getGoogleAuthOptions(
  credentials: dataform.IBigQuery,
  authClient?: AuthClient,
): { authClient: AuthClient } | { credentials?: Record<string, unknown> } {
  return authClient
    ? { authClient }
    : { credentials: credentials.credentials && JSON.parse(credentials.credentials) };
}

export function createCatalogClientProvider(
  credentials: dataform.IBigQuery,
  authClient?: AuthClient,
): () => CatalogServiceClient {
  let client: CatalogServiceClient | undefined;
  return () => {
    if (!client) {
      client = new CatalogServiceClient({
        ...getGoogleAuthOptions(credentials, authClient),
        libName: DATAFORM_CLI_LIB_NAME,
        libVersion: version,
      });
    }
    return client;
  };
}

export class KnowledgeCatalogMetadataWriter {
  private readonly credentials: dataform.IBigQuery;
  private readonly clientProvider: CatalogClientProvider;
  private readonly stderr: IStderrLike;
  private readonly sleepFn: (millis: number) => Promise<void>;
  private readonly disabledProjects = new Set<string>();
  private readonly pending = new Set<Promise<void>>();

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

  public writeMetadata(
    target: dataform.ITarget,
    location: string | undefined,
    metadata: dataform.IMetadata | null | undefined,
  ): Promise<void> {
    const p = this.writeMetadataInternal(target, location, metadata)
      .catch((e) => {
        const err = coerceAsError(e);
        const targetProject = target.database || this.credentials.projectId;
        const targetFqn = `${targetProject}.${target.schema}.${target.name}`;
        this.stderr.write(
          `Warning: Failed to write Knowledge Catalog metadata for ${targetFqn}: ${err.message}\n`,
        );
      })
      .finally(() => {
        this.pending.delete(p);
      });
    this.pending.add(p);
    return p;
  }

  public async drain(): Promise<void> {
    while (this.pending.size > 0) {
      await Promise.allSettled([...this.pending]);
    }
  }

  private async writeMetadataInternal(
    target: dataform.ITarget,
    location: string | undefined,
    metadata: dataform.IMetadata | null | undefined,
  ): Promise<void> {
    const targetProject = target.database || this.credentials.projectId;
    if (this.disabledProjects.has(targetProject)) {
      return;
    }

    if (!buildCatalogAspects(metadata)) {
      return;
    }

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

    const client = this.clientProvider();
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
        if (!entryNotFound && isPermissionDeniedError(e)) {
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

        const err = coerceAsError(e);
        if (isPermissionDeniedError(e)) {
          if (!this.disabledProjects.has(targetProject)) {
            this.disabledProjects.add(targetProject);
            this.stderr.write(
              `Warning: Skipping Knowledge Catalog metadata writes for project ${targetProject} ` +
                `for the rest of this run (failed on ${targetFqn}): ${err.message}\n`,
            );
          }
          return;
        }

        this.stderr.write(
          `Warning: Failed to write Knowledge Catalog metadata for ${targetFqn}: ${err.message}\n`,
        );
        return;
      }
    }
  }
}
