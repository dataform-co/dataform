import { expect } from "chai";
import * as fs from "fs";

import { CLI_COMMANDS } from "df/cli/commands";
import { generateReadme, UPDATE_README_COMMAND } from "df/cli/readme_generator";
import { suite, test } from "df/testing";

suite("cli/README.md", () => {
  test("generated blocks are up to date with the CLI definitions", () => {
    const readme = fs.readFileSync("cli/README.md", "utf8");
    // Content outside the generated blocks is passed through unchanged, so the README is up to date
    // exactly when no generated block changed.
    const { changedBlocks } = generateReadme(readme, CLI_COMMANDS);
    expect(
      changedBlocks,
      `cli/README.md is out of date with the CLI definitions in cli/. Generated blocks must not be ` +
        `edited by hand: change the command or option definitions instead, then run: ` +
        `${UPDATE_README_COMMAND}. Out-of-date blocks`,
    ).deep.equals([]);
  });
});
