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
 */
export class LineageEndpointRouter {
  private readonly repUnavailableForLocation = new Set<string>();

  public endpointForLocation(location: string): string {
    if (this.repUnavailableForLocation.has(location)) {
      return GLOBAL_LINEAGE_ENDPOINT;
    }
    return regionalEndpointFor(location);
  }

  public markRepUnavailable(location: string): void {
    this.repUnavailableForLocation.add(location);
  }
}
