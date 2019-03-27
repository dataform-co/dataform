import { expect } from "chai";
import {
  getHookBefore,
  getTestConfig,
  getTestRunCommand,
  queryRun
} from "df/tests/integration/utils";
import { asPlainObject } from "df/tests/utils";

describe("@dataform/integration/bigquery", function() {
  const testConfig = getTestConfig("bigquery");

  // check project credentials
  this.pending = !testConfig.credentials;
  if (this.isPending()) {
    console.log("No Bigquery credentials, tests will be skipped!");
  }

  describe("run", function() {
    this.timeout(300000);
    const expectedResult = [
      { id: "example_backticks", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_js_blocks", data: [{ foo: 1 }] },
      { id: "example_deferred", data: [{ test: 1 }] },
      { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_using_inline", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_incremental", data: [] }
    ];

    before("clear_schema", getHookBefore(testConfig));

    it("bigquery_1", getTestRunCommand(testConfig, expectedResult, 1));

    it("bigquery_2", getTestRunCommand(testConfig, expectedResult, 2));
  });

  describe("cancelable_query_run", function() {
    this.timeout(60000);
    const sql = "select 1 as test union all select 2 as test union all select 3 as test";
    const expectedResult = [{ test: 1 }, { test: 2 }, { test: 3 }];

    it("not_canceled", () => {
      return queryRun(sql, testConfig).then(result => {
        expect(asPlainObject(result)).deep.equals(asPlainObject(expectedResult));
      });
    });

    it("canceled", async () => {
      const promise = queryRun(sql, testConfig);
      setTimeout(() => promise.cancel(), 10);
      try {
        const result = await promise;
        throw new Error("Should not pass");
      } catch (err) {
        expect(err).to.be.an.instanceof(Error);
        expect(err.message).to.equals("Query cancelled.");
      }
    });
  });
});
