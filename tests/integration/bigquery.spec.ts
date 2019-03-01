import { expect } from "chai";
import { getTestConfig, getHookBefore, getTestRunCommand, queryRun } from "df/tests/integration/utils";
import { asPlainObject } from "df/tests/utils";

describe("@dataform/integration/bigquery", function() {
  const testConfig = getTestConfig("bigquery");

  // check project credentials
  this.pending = !testConfig.profile;
  if (this.isPending()) {
    console.log("No Bigquery profile config, tests will be skipped!");
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

    it("canceled", () => {
      return new Promise((resolve, reject) => {
        const promise = queryRun(sql, testConfig)
          .then(resolve)
          .finally(() => {
            if (promise.isCancelled()) reject(new Error("Run cancelled"));
          });

        promise.cancel();
      })
        .then(result => {
          // if the cancellation did not work - check the results
          expect(asPlainObject(result)).not.deep.equals(asPlainObject(expectedResult));
        })
        .catch(err => {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.equals("Run cancelled");
        });
    });
  });
});
