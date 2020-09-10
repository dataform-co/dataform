import { expect } from "chai";

import { suite, test } from "df/testing";
import { PrestoFixture } from "df/tools/presto/presto_fixture";

suite("@dataform/integration/presto", { parallel: true }, ({ before, after }) => {
  const _ = new PrestoFixture(1234, before, after);

  test("connection", { timeout: 60000 }, async () => {
    // const result = await prestoExecute({ query: "SELECT 1" });
    // expect(result.stats.state).to.equal("FINISHED");
    // expect(result.error).to.equal(null);
    // expect(result.data).to.deep.equal([[1]]);
    expect(true).to.equal(false);
  });
});
