import { expect } from "chai";
import * as fs from "fs";

import { CLI_COMMANDS } from "df/cli/commands";
import { formatStaleBlocks, generateReadme } from "df/cli/readme_generator";
import { suite, test } from "df/testing";

suite("cli/README.md", () => {
  test("generated blocks are up to date with the CLI definitions", () => {
    // Bazel runs js_test with the runfiles root as the working directory, and README.md is a
    // `data` dependency of this test, so the workspace-relative path resolves to it.
    const readme = fs.readFileSync("cli/README.md", "utf8");
    // Content outside the generated blocks is passed through unchanged, so the README is up to date
    // exactly when no generated block changed.
    const { staleBlocks } = generateReadme(readme, CLI_COMMANDS);
    if (staleBlocks.length > 0) {
      expect.fail(formatStaleBlocks(staleBlocks));
    }
  });
});
