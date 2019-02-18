import { getTestConfig, getHookBefore, getTestRunCommand } from "df/tests/integration/utils";

describe("@dataform/integration/redshift", function() {
    this.timeout(300000);

    const testConfig = getTestConfig("redshift");
    const expectedResult = [
      { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_table_dependency", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_incremental", data: [] }
    ];

    // check project credentials
    this.pending = !testConfig.profile;
    if (this.isPending()) {
      console.log("No Redshift profile config, tests will be skipped!");
    }

    before("clear_schema", getHookBefore(testConfig));

    it("redshift_1", getTestRunCommand(testConfig, expectedResult, 1));

    it("redshift_2", getTestRunCommand(testConfig, expectedResult, 2));
});
