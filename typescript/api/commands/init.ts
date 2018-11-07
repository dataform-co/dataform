import * as path from "path";
import * as fs from "fs";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import { install } from "./install";

export function init(
  projectDir: string,
  warehouse: string,
  projectName?: string
) {
  var dataformJsonPath = path.join(projectDir, "dataform.json");
  var packageJsonPath = path.join(projectDir, "package.json");
  var gitignorePath = path.join(projectDir, ".gitignore");
  if (fs.existsSync(dataformJsonPath) || fs.existsSync(packageJsonPath)) {
    throw "Cannot init dataform project, this already appears to be an NPM or Dataform directory.";
  }
  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir);
  }
  fs.writeFileSync(
    dataformJsonPath,
    JSON.stringify(
      protos.ProjectConfig.create({
        warehouse: warehouse,
        defaultSchema: "dataform",
        assertionSchema: "dataform_asserts"
      }),
      null,
      4
    ) + "\n"
  );
  fs.writeFileSync(
    packageJsonPath,
    JSON.stringify(
      {
        name: projectName || utils.baseFilename(path.resolve(projectDir)),
        dependencies: {}
      },
      null,
      4
    ) + "\n"
  );
  fs.writeFileSync(
    gitignorePath,
    `node_modules/
  `
  );
  // Make the default models, includes folders.
  fs.mkdirSync(path.join(projectDir, "models"));
  fs.mkdirSync(path.join(projectDir, "includes"));

  // Install packages.
  return install(projectDir);
}
