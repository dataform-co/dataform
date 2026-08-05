export const GLOBAL_LINEAGE_ENDPOINT = "datalineage.googleapis.com";

function regionalEndpointFor(location: string): string {
  return `datalineage.${location}.rep.googleapis.com`;
}

/**
 * Routes lineage RPCs to the correct endpoint for a given location.
 *
 * Default policy is REP-first (`datalineage.<loc>.rep.googleapis.com`) with
 * fallback to global (`datalineage.googleapis.com`) once a location is marked
 * unavailable via `markRepUnavailable`. Fallback is sticky for the lifetime
 * of the router — subsequent emits in the same location skip the REP endpoint.
 *
 * When an explicit override is set (via `--api-endpoint` / IEmitterOptions.
 * apiEndpoint), it is returned unconditionally and the REP-fallback logic is
 * bypassed. This is intended for e2e tests and staging environments.
 */
export class LineageEndpointRouter {
  private readonly override?: string;
  private readonly repUnavailableForLocation = new Set<string>();

  constructor(override?: string) {
    this.override = override;
  }

  public endpointForLocation(location: string): string {
    if (this.override) {
      return this.override;
    }
    if (this.repUnavailableForLocation.has(location)) {
      return GLOBAL_LINEAGE_ENDPOINT;
    }
    return regionalEndpointFor(location);
  }

  public markRepUnavailable(location: string): void {
    this.repUnavailableForLocation.add(location);
  }

  public isUsingOverride(): boolean {
    return !!this.override;
  }
}
