import * as fs from "fs";
import * as path from "path";

import { CLI_COMMANDS } from "df/cli/commands";
import { printError, printSuccess } from "df/cli/console";
import { generateReadme } from "df/cli/readme_generator";

// Regenerates the generated blocks of cli/README.md from the CLI command definitions.
// Usage: bazel run //cli:update_readme
const workspaceDir = process.env.BUILD_WORKSPACE_DIRECTORY;
if (!workspaceDir) {
  printError("This script must be run with: bazel run //cli:update_readme");
  process.exit(1);
}

const readmePath = path.join(workspaceDir, "cli", "README.md");
const { content, changedBlocks } = generateReadme(
  fs.readFileSync(readmePath, "utf8"),
  CLI_COMMANDS,
);

if (changedBlocks.length > 0) {
  fs.writeFileSync(readmePath, content);
  printSuccess(`Updated ${readmePath}: ${changedBlocks.join(", ")}`);
} else {
  // Not written, to avoid touching the file's mtime (and triggering watchers or rebuilds).
  printSuccess(`${readmePath} is already up to date.`);
}
