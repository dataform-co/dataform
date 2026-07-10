import { LineageClient } from "@google-cloud/lineage";

export interface IRecordedCall {
  request: any;
  timestamp: number;
}

export interface IProviderCall {
  projectId: string;
  endpoint: string;
}

type NextResponse =
  | { kind: "ok" }
  | { kind: "throw"; code: number; message?: string }
  | { kind: "hang" };

/**
 * Test double for `@google-cloud/lineage`'s LineageClient that records every
 * processOpenLineageRunEvent call for later assertion, and can be programmed
 * to fail or hang on the next call. Used by lineage integration tests to
 * observe emitted OpenLineage payloads without hitting the real API.
 *
 * The recorder is the load-bearing observable — assertions target
 * `recorder.calls`, never stderr message strings.
 */
export class RecordingLineageClient {
  public readonly calls: IRecordedCall[] = [];
  public readonly providerCalls: IProviderCall[] = [];
  private nextResponse: NextResponse = { kind: "ok" };

  public async processOpenLineageRunEvent(request: any): Promise<any> {
    this.calls.push({ request, timestamp: Date.now() });
    if (this.nextResponse.kind === "throw") {
      const err: any = new Error(this.nextResponse.message || `injected code=${this.nextResponse.code}`);
      err.code = this.nextResponse.code;
      throw err;
    }
    if (this.nextResponse.kind === "hang") {
      await new Promise(() => { /* never resolves */ });
    }
    const runId = request?.openLineage?.fields?.run?.structValue?.fields?.runId?.stringValue || "unknown";
    return { name: `projects/test/locations/us/processes/${runId}` };
  }

  public throwOnNextCallWith(grpcCode: number, message?: string): void {
    this.nextResponse = { kind: "throw", code: grpcCode, message };
  }

  public hangForever(): void {
    this.nextResponse = { kind: "hang" };
  }

  public reset(): void {
    this.calls.length = 0;
    this.providerCalls.length = 0;
    this.nextResponse = { kind: "ok" };
  }
}

/**
 * Returns a LineageClientProvider-shaped factory that always yields the given
 * recorder, and remembers every (projectId, endpoint) pair the emitter asks
 * for so tests can assert on endpoint selection (case 5 in T37).
 */
export function recorderProvider(
  recorder: RecordingLineageClient
): (projectId: string, endpoint: string) => LineageClient {
  return (projectId: string, endpoint: string) => {
    recorder.providerCalls.push({ projectId, endpoint });
    return recorder as unknown as LineageClient;
  };
}
