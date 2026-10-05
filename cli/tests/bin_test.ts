import { expect } from "chai";
import { execFile } from "child_process";
import { cliEntryPointPath } from "df/cli/index_test_base";
import { getProcessResult, suite, test } from "df/testing";

suite("binary run", () => {
  test("CLI runs successfully when invoked directly as a binary", async () => {
    const result = await getProcessResult(execFile(cliEntryPointPath));
    expect(result.exitCode).equals(0);
    const err = result.stderr;
    expect(err).to.include("dataform [command]");
  });
});
