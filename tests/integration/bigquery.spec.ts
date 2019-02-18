import { getTestConfig, getHookBefore, getTestRunCommand } from "df/tests/integration/utils";

describe("@dataform/integration/bigquery", function() {
  this.timeout(300000);

  const testConfig = getTestConfig("bigquery");
  const expectedResult = [
    { id: "example_backticks", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
    { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
    { id: "example_js_blocks", data: [{ foo: 1 }] },
    { id: "example_deferred", data: [{ test: 1 }] },
    { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
    { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
    { id: "example_incremental", data: [] }
  ];

  // check project credentials
  this.pending = !testConfig.profile;
  if (this.isPending()) {
    console.log("No Bigquery profile config, tests will be skipped!");
  }

  before("clear_schema", getHookBefore(testConfig));

  it("bigquery_1", getTestRunCommand(testConfig, expectedResult, 1));

  it("bigquery_2", getTestRunCommand(testConfig, expectedResult, 2));
});
