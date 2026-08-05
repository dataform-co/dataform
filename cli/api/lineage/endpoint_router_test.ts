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

  test("override wins over REP and REP-unavailable state", () => {
    const router = new LineageEndpointRouter("custom.example.com");
    router.markRepUnavailable("us");
    expect(router.endpointForLocation("us")).to.equal("custom.example.com");
    expect(router.endpointForLocation("eu")).to.equal("custom.example.com");
  });

  test("isUsingOverride reflects override state", () => {
    expect(new LineageEndpointRouter().isUsingOverride()).to.equal(false);
    expect(new LineageEndpointRouter("").isUsingOverride()).to.equal(false);
    expect(new LineageEndpointRouter("custom.example.com").isUsingOverride()).to.equal(true);
  });
});
