import * as path from "path";
import * as fs from "fs";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import { install } from "./install";
import { version } from "../package.json";

export function init(projectDir: string, projectConfig: protos.IProjectConfig) {
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
      Object.assign(
        {},
        protos.ProjectConfig.create({
          defaultSchema: "dataform",
          assertionSchema: "dataform_assertions"
        }),
        projectConfig
      ),
      null,
      4
    ) + "\n"
  );
  fs.writeFileSync(
    packageJsonPath,
    JSON.stringify(
      {
        dependencies: {
          "@dataform/core": version
        }
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
  fs.mkdirSync(path.join(projectDir, "definitions"));
  fs.mkdirSync(path.join(projectDir, "includes"));

  // Install packages.
  return install(projectDir);
}
