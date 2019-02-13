import { getTestConfig, getHookBefore, getTestRunCommand } from "df/tests/integration/utils";

describe("@dataform/integration/snowflake", function() {
  this.timeout(300000);

  const testConfig = getTestConfig("snowflake");
  const expectedResult = [
    { id: "example_table", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
    { id: "example_view", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
    { id: "sample_data", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
    { id: "example_incremental", data: [] }
  ];

  // check project credentials
  this.pending = !testConfig.profile;
  if (this.isPending()) {
    console.log("No Snowflake profile config, tests will be skipped!");
  }

  before("clear_schema", getHookBefore(testConfig));

  it("snowflake_1", getTestRunCommand(testConfig, expectedResult, 1));

  it("snowflake_2", getTestRunCommand(testConfig, expectedResult, 2));
});
