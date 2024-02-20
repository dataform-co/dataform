import * as fs from "fs";
import * as path from "path";
import { promisify } from "util";

import * as childProcess from "child_process";
import { readDataformCoreVersionFromWorkflowSettings } from "df/cli/api/utils";

export const MISSING_CORE_VERSION_ERROR =
  "dataformCoreVersion must be specified either in workflow_settings.yaml or via a package.json";

export async function install(projectPath: string) {
  const resolvedProjectPath = path.resolve(projectPath);
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");

  // Core's readWorkflowSettings method cannot be used for this because Core assumes that
  // `require` can read YAML files directly.
  const dataformCoreVersion = readDataformCoreVersionFromWorkflowSettings(resolvedProjectPath);
  if (dataformCoreVersion) {
    throw new Error(
      "Package installation is only supported when specifying @dataform/core version in " +
        "'package.json'"
    );
  }

  if (!fs.existsSync(packageJsonPath)) {
    throw new Error(MISSING_CORE_VERSION_ERROR);
  }

  await promisify(childProcess.exec)("npm i --ignore-scripts", { cwd: resolvedProjectPath });
}
