import * as fs from "fs";
import * as path from "path";

/**
 * Returns the path of the `@dataform/core` bundle installed in `projectDir`, or throws an error
 * that explains why it is missing. Compilation and JiT compilation both load `@dataform/core`
 * from `projectDir`.
 */
export function findInstalledCoreBundle(projectDir: string): string {
  const coreBundlePath = path.join(projectDir, "node_modules", "@dataform", "core", "bundle.js");
  if (fs.existsSync(coreBundlePath)) {
    return coreBundlePath;
  }
  if (!fs.existsSync(projectDir)) {
    throw new Error(
      `Could not find @dataform/core: the project directory '${projectDir}' does not exist.`,
    );
  }
  if (fs.existsSync(path.join(projectDir, "package.json"))) {
    throw new Error(
      `Could not find @dataform/core: '${coreBundlePath}' does not exist. Run ` +
        "`dataform install` in the project directory to install the version from package.json.",
    );
  }
  // Without package.json, @dataform/core comes from `dataformCoreVersion` in
  // workflow_settings.yaml. The CLI installs it into a temporary copy of the project, so the
  // original project directory never contains it.
  throw new Error(
    `Could not find @dataform/core: '${coreBundlePath}' does not exist. Projects that set ` +
      "`dataformCoreVersion` in workflow_settings.yaml are compiled in a temporary directory " +
      "where that version is installed; compilation and JiT compilation must run in that " +
      "directory, not in the project directory.",
  );
}
