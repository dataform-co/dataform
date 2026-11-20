import * as fs from "fs";
import * as path from "path";

import {
  defaultCoreSourcePaths,
  generateCoreReadme,
  loadCoreDocData,
  UPDATE_README_COMMAND,
} from "df/tools/docs/core_readme_generator";

// Regenerates the generated blocks of core/README.md from @dataform/core TypeScript and .proto
// definitions.
// Usage: bazel run //core:update_readme
const workspaceDir = process.env.BUILD_WORKSPACE_DIRECTORY;
if (!workspaceDir) {
  process.stderr.write(`This script must be run with: ${UPDATE_README_COMMAND}\n`);
  process.exit(1);
}

const readmePath = path.join(workspaceDir, "core", "README.md");
const data = loadCoreDocData(defaultCoreSourcePaths(workspaceDir));
const { content, changedBlocks } = generateCoreReadme(fs.readFileSync(readmePath, "utf8"), data);

if (changedBlocks.length > 0) {
  // Replace via rename so any read-only Bazel hardlink is unlinked cleanly and mode stays 0644.
  const tmpPath = `${readmePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmpPath, content, { mode: 0o644 });
  fs.renameSync(tmpPath, readmePath);
  process.stdout.write(`Updated ${readmePath}: ${changedBlocks.join(", ")}\n`);
} else {
  // Not written, to avoid touching the file's mtime (and triggering watchers or rebuilds).
  process.stdout.write(`${readmePath} is already up to date.\n`);
}
