import { expect } from "chai";
import * as fs from "fs";

import { suite, test } from "df/testing";
import { formatStaleCoreBlocks, generateCoreReadme } from "df/tools/docs/core_readme_generator";

suite("core/README.md generated blocks", () => {
  test("are up to date with core/ and protos/ definitions", () => {
    const readme = fs.readFileSync("core/README.md", "utf8");
    const { staleBlocks } = generateCoreReadme(readme);
    if (staleBlocks.length > 0) {
      expect.fail(formatStaleCoreBlocks(staleBlocks));
    }
  });
});
