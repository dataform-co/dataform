import { LineageClient } from "@google-cloud/lineage";

import {
  GLOBAL_LINEAGE_ENDPOINT,
  LineageEndpointRouter
} from "df/cli/api/lineage/endpoint_router";
import { LineagePayloadBuilder, toProtoStruct } from "df/cli/api/lineage/payload_builder";
import { coerceAsError } from "df/common/errors/errors";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

export interface IEmitterOptions {
  lineageEnabled?: boolean;
  dryRun?: boolean;
  projectDir?: string;
}

/**
 * Minimal writable stream shape used for stderr output. Injectable so tests can
 * capture emitted skip-reason lines deterministically.
 */
export interface IStderrLike {
  write(msg: string): unknown;
}

export type LineageClientProvider = (projectId: string, endpoint: string) => LineageClient;

const DATAFORM_CLI_LIB_NAME = "dataform-cli";

export function createLineageClientProvider(
  credentials: dataform.IBigQuery
): LineageClientProvider {
  const clients = new Map<string, LineageClient>();
  return (projectId: string, endpoint: string) => {
    const targetProjectId = projectId || credentials.projectId;
    const cacheKey = `${targetProjectId}::${endpoint}`;
    if (!clients.has(cacheKey)) {
      clients.set(
        cacheKey,
        new LineageClient({
          projectId: targetProjectId,
          apiEndpoint: endpoint,
          credentials: credentials.credentials && JSON.parse(credentials.credentials),
          libName: DATAFORM_CLI_LIB_NAME,
          libVersion: version
        })
      );
    }
    return clients.get(cacheKey);
  };
}

const GRPC_CODE_NAMES: { [k: number]: string } = {
  0: "OK",
  1: "CANCELLED",
  2: "UNKNOWN",
  3: "INVALID_ARGUMENT",
  4: "DEADLINE_EXCEEDED",
  5: "NOT_FOUND",
  6: "ALREADY_EXISTS",
  7: "PERMISSION_DENIED",
  8: "RESOURCE_EXHAUSTED",
  9: "FAILED_PRECONDITION",
  10: "ABORTED",
  11: "OUT_OF_RANGE",
  12: "UNIMPLEMENTED",
  13: "INTERNAL",
  14: "UNAVAILABLE",
  15: "DATA_LOSS",
  16: "UNAUTHENTICATED"
};

function gRpcCodeName(code: number | undefined): string {
  if (code === undefined) {
    return "UNKNOWN";
  }
  return GRPC_CODE_NAMES[code] || "UNKNOWN";
}

/**
 * Retry policy for ProcessOpenLineageRunEvent, delegated to google-gax.
 *
 * Transient set per gRPC canonical retry semantics (https://google.aip.dev/194):
 *   4  DEADLINE_EXCEEDED    – RPC timed out
 *   8  RESOURCE_EXHAUSTED   – quota/rate-limit (needs backoff, hence gax)
 *   10 ABORTED              – concurrency conflict
 *   13 INTERNAL             – transient server error
 *   14 UNAVAILABLE          – server briefly unreachable
 *
 * Backoff: 1s / 2s / 4s exponential; total budget 15s. Per-attempt RPC
 * timeout 2s (matches prior CallOptions.timeout).
 *
 * REP→global endpoint fallback is NOT expressed here — gax cannot reconfigure
 * the client's endpoint on error. That logic stays in the outer try/catch in
 * emitOpenLineageEvent.
 */
export const LINEAGE_RETRY_CONFIG = {
  retryCodes: [4, 8, 10, 13, 14],
  backoffSettings: {
    initialRetryDelayMillis: 1000,
    retryDelayMultiplier: 2.0,
    maxRetryDelayMillis: 4000,
    initialRpcTimeoutMillis: 2000,
    rpcTimeoutMultiplier: 1.0,
    maxRpcTimeoutMillis: 2000,
    totalTimeoutMillis: 15000
  }
};

export class LineageEmitter {
  private readonly clientProvider: LineageClientProvider;
  private readonly credentials: dataform.IBigQuery;
  private readonly emitterOptions: IEmitterOptions;
  private readonly stderr: IStderrLike;
  private readonly payloadBuilder: LineagePayloadBuilder;
  private readonly endpointRouter: LineageEndpointRouter;
  private readonly pending = new Set<Promise<void>>();
  private emissionDisabledThisRun = false;
  private dryRunSkipLogged = false;

  constructor(
    credentials: dataform.IBigQuery,
    emitterOptions: IEmitterOptions,
    clientProvider?: LineageClientProvider,
    stderr: IStderrLike = process.stderr
  ) {
    this.credentials = credentials;
    this.emitterOptions = emitterOptions;
    this.stderr = stderr;
    this.clientProvider = clientProvider || createLineageClientProvider(credentials);
    this.payloadBuilder = new LineagePayloadBuilder(emitterOptions.projectDir);
    this.endpointRouter = new LineageEndpointRouter();
  }

  public emitForAction(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): void {
    if (this.emissionDisabledThisRun) {
      return;
    }

    if (this.emitterOptions.dryRun) {
      if (!this.dryRunSkipLogged) {
        this.stderr.write(
          "[lineage] Skipped lineage emission (dry-run mode; once-per-run): skip_reason=dry_run\n"
        );
        this.dryRunSkipLogged = true;
      }
      return;
    }

    // Non-table/non-operation actions (e.g., assertions, declarations) are not
    // emitted. This is a scope decision, not a user-visible misconfiguration,
    // so it is intentionally silent.
    const isEligibleType = action.type === "table" || action.type === "operation";
    if (!isEligibleType) {
      return;
    }

    const p = this.emitForActionInternal(action, actionResult)
      .catch(e => {
        const code = (e as any).code;
        const endpoint = (e as any).lineageEndpoint || "unknown";
        const location = (e as any).lineageLocation || "unknown";
        this.stderr.write(
          `[lineage] Failed to emit lineage for action ${action.target.schema}.${action.target.name}: ` +
            `code=${gRpcCodeName(code)}(${code ?? "?"}) endpoint=${endpoint} location=${location} message=${e.message}\n`
        );
      })
      .finally(() => {
        this.pending.delete(p);
      });
    this.pending.add(p);
  }

  public async drain(maxWaitMs = 15000): Promise<void> {
    if (this.pending.size === 0) {
      return;
    }
    await Promise.race([
      Promise.allSettled([...this.pending]),
      new Promise<void>(resolve => setTimeout(resolve, maxWaitMs))
    ]);
  }

  private async emitForActionInternal(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): Promise<void> {
    const projectId = action.target.database || this.credentials.projectId;
    const location = (this.credentials.location || "US").toLowerCase();
    const parent = `projects/${projectId}/locations/${location}`;

    const openLineagePayload = this.payloadBuilder.build(
      action,
      actionResult,
      projectId,
      location,
      this.credentials.projectId
    );

    // Emit payload via ProcessOpenLineageRunEvent. Retry policy is delegated
    // to google-gax (see LINEAGE_RETRY_CONFIG). Outer loop only handles
    // behaviors gax cannot express: REP→global endpoint fallback (requires
    // reconfiguring the client) and PERMISSION_DENIED / SERVICE_DISABLED
    // skip-with-reason (error-swallow-with-stderr).
    // Runs at most twice — initial invocation + one REP fallback.
    let currentEndpoint = this.endpointRouter.endpointForLocation(location);
    for (let repFallbackAttempts = 0; repFallbackAttempts < 2; repFallbackAttempts++) {
      try {
        const client = this.clientProvider(projectId, currentEndpoint);
        await client.processOpenLineageRunEvent(
          {
            parent,
            openLineage: toProtoStruct(openLineagePayload) as any
          },
          { retry: LINEAGE_RETRY_CONFIG }
        );
        return;
      } catch (e) {
        const err = coerceAsError(e);
        const code = (err as any).code;

        // Fall back from REP to global when the endpoint isn't serving this
        // location — either unresolvable via DNS or reachable-but-returning
        // an HTTP 302 (GFE routing signal that the endpoint has no route for
        // this region).
        const onRepEndpoint = currentEndpoint !== GLOBAL_LINEAGE_ENDPOINT;
        if (
          onRepEndpoint &&
          (this.isEndpointUnresolvable(err) || this.isEndpointRegionMismatch(err))
        ) {
          this.stderr.write(
            `[lineage] Regional endpoint ${currentEndpoint} is not serving location ${location}. Falling back to ${GLOBAL_LINEAGE_ENDPOINT} for this and subsequent emits in this location.\n`
          );
          this.endpointRouter.markRepUnavailable(location);
          currentEndpoint = GLOBAL_LINEAGE_ENDPOINT;
          continue;
        }

        // Endpoint returned HTTP 302 (region mismatch) and we can't retry from
        // here — skip the rest of this run so we don't repeat a guaranteed
        // failure for every action.
        if (this.isEndpointRegionMismatch(err)) {
          if (!this.emissionDisabledThisRun) {
            this.emissionDisabledThisRun = true;
            this.stderr.write(
              `[lineage] Skipped lineage emission for the rest of this run: skip_reason=endpoint_region_mismatch (endpoint '${currentEndpoint}' returned HTTP 302 for location '${location}'; the endpoint does not serve this region)\n`
            );
          }
          return;
        }

        // Check for permission or API disabled status codes. Multiple in-flight
        // calls can hit the same failure concurrently; guard the write so the
        // skip line is printed at most once per run.
        //
        // PERMISSION_DENIED is ambiguous on Google Cloud: it fires both for
        // missing IAM and for "API not enabled" (the enablement check often
        // returns 7 rather than 9). Surface both hints so the user doesn't
        // chase one root cause when the other is at fault.
        if (code === 7 || err.message?.includes("PERMISSION_DENIED")) {
          if (!this.emissionDisabledThisRun) {
            this.emissionDisabledThisRun = true;
            this.stderr.write(
              `[lineage] Skipped lineage emission for the rest of this run: skip_reason=api_disabled (ensure the credential has 'datalineage.googleapis.com/locations.processOpenLineageMessage' OR that the Lineage API is enabled in project ${projectId} via 'gcloud services enable datalineage.googleapis.com')\n`
            );
          }
          return;
        }
        if (
          code === 9 ||
          err.message?.includes("SERVICE_DISABLED") ||
          err.message?.includes("FAILED_PRECONDITION")
        ) {
          if (!this.emissionDisabledThisRun) {
            this.emissionDisabledThisRun = true;
            this.stderr.write(
              `[lineage] Skipped lineage emission for the rest of this run: skip_reason=api_disabled (Lineage API is not enabled in project ${projectId}; run 'gcloud services enable datalineage.googleapis.com')\n`
            );
          }
          return;
        }
        // UNAUTHENTICATED is non-recoverable within a single CLI run — the
        // credential is loaded once at startup and does not refresh mid-run,
        // so continuing to emit would just produce N identical failure lines.
        // Flip the same kill-switch as 7/9 and print exactly one hint.
        if (code === 16 || err.message?.includes("UNAUTHENTICATED")) {
          if (!this.emissionDisabledThisRun) {
            this.emissionDisabledThisRun = true;
            this.stderr.write(
              `[lineage] Skipped lineage emission for the rest of this run: skip_reason=unauthenticated (the credential used to reach the Lineage API is missing, invalid, or expired; re-authenticate and rerun — e.g., 'gcloud auth application-default login')\n`
            );
          }
          return;
        }

        // gax has already exhausted retries on transient codes; propagate to
        // the outer catch with endpoint/location metadata attached so the
        // structured `[lineage] Failed to emit ...` line can name them.
        (err as any).lineageEndpoint = currentEndpoint;
        (err as any).lineageLocation = location;
        throw err;
      }
    }
  }

  private isEndpointUnresolvable(err: Error): boolean {
    // Match on the DNS signature in the message string rather than the outer
    // grpc code. google-gax wraps repeated UNAVAILABLE(14) errors from the
    // grpc DNS resolver in an outer DEADLINE_EXCEEDED(4) once its retry
    // budget expires; the inner "Name resolution failed" text is only in the
    // message. The signatures below are unique enough on their own that a
    // false positive is not a concern.
    const cause = (err as any).cause;
    const causeMessage =
      typeof cause === "object" && cause ? String(cause.message || cause.code || "") : "";
    const combined = `${err.message || ""} ${causeMessage}`;
    return /ENOTFOUND|EAI_AGAIN|getaddrinfo|(?:DNS|Name) resolution failed/i.test(combined);
  }

  private isEndpointRegionMismatch(err: Error): boolean {
    // HTTP 302 to a gRPC client is a GFE / uberproxy redirect — the endpoint
    // has no backend route for this region, so the frontend responds with a
    // Location header instead of gRPC frames. gRPC surfaces this as
    // UNKNOWN(2) because there is no canonical status attached.
    if ((err as any).code !== 2) {
      return false;
    }
    const cause = (err as any).cause;
    const causeMessage =
      typeof cause === "object" && cause ? String(cause.message || cause.code || "") : "";
    const combined = `${err.message || ""} ${causeMessage}`;
    // Two shapes seen in practice: "Received http2 header with status: 302"
    // (grpc-js internal wrap) and "302:Found" (stringified HTTP status, as
    // returned by our observed live run).
    return /Received http2 header with status: 30[12]|\b30[12]:\s*Found\b/i.test(combined);
  }
}
