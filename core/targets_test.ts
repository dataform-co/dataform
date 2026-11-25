import { expect } from "chai";

import { resolvableAsActionConfigTarget } from "df/core/targets";
import { suite, test } from "df/testing";

suite("targets", () => {
  test("resolvableAsActionConfigTarget supports Lakehouse objects and rejects 4-part dotted strings", () => {
    expect(
      resolvableAsActionConfigTarget({
        project: "p",
        catalog: "c",
        namespace: "n",
        name: "t",
      }),
    ).to.deep.equal({
      name: "t",
      project: "p",
      lakehouse: {
        catalog: "c",
        namespace: "n",
      },
      includeDependentAssertions: undefined,
    });
    expect(() => resolvableAsActionConfigTarget("p.c.n.t")).to.throw(
      "Invalid dependency. Accepted inputs include",
    );
    expect(() => resolvableAsActionConfigTarget("example.com:proj.d.t")).to.throw(
      "Invalid dependency. Accepted inputs include",
    );
  });
});
