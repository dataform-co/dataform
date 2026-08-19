import { expect } from "chai";

import {
  GLOBAL_LINEAGE_ENDPOINT,
  LineageEndpointRouter
} from "df/cli/api/lineage/endpoint_router";
import { suite, test } from "df/testing";

suite("LineageEndpointRouter", () => {
  test("returns REP endpoint by default", () => {
    const router = new LineageEndpointRouter();
    expect(router.endpointForLocation("us")).to.equal("datalineage.us.rep.googleapis.com");
    expect(router.endpointForLocation("eu")).to.equal("datalineage.eu.rep.googleapis.com");
  });

  test("returns global endpoint after markRepUnavailable, for that location only", () => {
    const router = new LineageEndpointRouter();
    router.markRepUnavailable("us");
    expect(router.endpointForLocation("us")).to.equal(GLOBAL_LINEAGE_ENDPOINT);
    expect(router.endpointForLocation("eu")).to.equal("datalineage.eu.rep.googleapis.com");
  });
});
