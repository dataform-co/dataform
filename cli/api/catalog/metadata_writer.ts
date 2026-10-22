import { CatalogServiceClient } from "@google-cloud/dataplex";
import { AuthClient } from "google-auth-library";

import {
  buildCatalogAspects,
  buildUpdateEntryRequest,
  isMultiRegionLocation,
} from "df/cli/api/catalog/request_builder";
import type { IStderrLike } from "df/cli/api/lineage/emitter";
import { coerceAsError } from "df/common/errors/errors";
import { sleep } from "df/common/promises";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

export type ICatalogClient = Pick<CatalogServiceClient, "updateEntry">;
export type CatalogClientProvider = (projectId?: string) => ICatalogClient;

const DATAFORM_CLI_LIB_NAME = "dataform-cli";
const NOT_FOUND_MAX_ATTEMPTS = 5;
const NOT_FOUND_INITIAL_DELAY_MILLIS = 1000;

export const CATALOG_RETRY_CONFIG = {
  retryCodes: [
    4, // DEADLINE_EXCEEDED
    8, // RESOURCE_EXHAUSTED
    13, // INTERNAL
    14, // UNAVAILABLE
  ],
  backoffSettings: {
    initialRetryDelayMillis: 100,
    retryDelayMultiplier: 1.3,
    maxRetryDelayMillis: 5000,
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

    if (!trimmedLocation || isMultiRegionLocation(trimmedLocation)) {
      this.stderr.write(
        trimmedLocation
          ? `Warning: Skipping Knowledge Catalog metadata write for ${targetFqn}: ` +
              `multi-region location "${trimmedLocation}" is not supported by Knowledge Catalog.\n`
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
        if (isNotFoundError(e) && attempt < NOT_FOUND_MAX_ATTEMPTS) {
          await this.sleepFn(delayMillis);
          delayMillis *= 2;
          continue;
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

    if (code === 7 || message.includes("PERMISSION_DENIED")) {
      if (!this.writesDisabledThisRun) {
        this.writesDisabledThisRun = true;
        this.stderr.write(
          `Warning: Skipping Knowledge Catalog metadata writes for the rest of this run ` +
            `(failed on ${targetFqn}): PERMISSION_DENIED (${message}). ` +
            `Ensure the credential has 'roles/dataplex.catalogEditor' or that the Dataplex API is enabled in project ${projectId}.\n`,
        );
      }
      return true;
    }

    if (
      code === 9 ||
      message.includes("SERVICE_DISABLED") ||
      message.includes("FAILED_PRECONDITION")
    ) {
      if (!this.writesDisabledThisRun) {
        this.writesDisabledThisRun = true;
        this.stderr.write(
          `Warning: Skipping Knowledge Catalog metadata writes for the rest of this run ` +
            `(failed on ${targetFqn}): Dataplex API is not enabled in project ${projectId} (${message}).\n`,
        );
      }
      return true;
    }

    if (code === 16 || message.includes("UNAUTHENTICATED")) {
      if (!this.writesDisabledThisRun) {
        this.writesDisabledThisRun = true;
        this.stderr.write(
          `Warning: Skipping Knowledge Catalog metadata writes for the rest of this run ` +
            `(failed on ${targetFqn}): UNAUTHENTICATED (${message}).\n`,
        );
      }
      return true;
    }

    return false;
  }
}
