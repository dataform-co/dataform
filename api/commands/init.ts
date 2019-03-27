import { dataform } from "@dataform/protos";
import * as fs from "fs";
import * as path from "path";
import { install } from "./install";

const { version } = require("../package.json");

export function init(
  projectDir: string,
  projectConfig: dataform.IProjectConfig,
  skipInstall?: boolean
): Promise<any> {
  const dataformJsonPath = path.join(projectDir, "dataform.json");
  const packageJsonPath = path.join(projectDir, "package.json");
  const gitignorePath = path.join(projectDir, ".gitignore");
  if (fs.existsSync(dataformJsonPath) || fs.existsSync(packageJsonPath)) {
    throw new Error(
      "Cannot init dataform project, this already appears to be an NPM or Dataform directory."
    );
  }

  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir);
  }
  fs.writeFileSync(
    dataformJsonPath,
    JSON.stringify(
      Object.assign(
        {},
        dataform.ProjectConfig.create({
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
  return install(projectDir, skipInstall);
}
