import { dataform } from "@dataform/protos";
import * as fs from "fs";
import * as path from "path";
import { install } from "@dataform/api/commands/install";

const { version } = require("../package.json");

export interface InitResult {
  filesWritten: string[];
  dirsCreated: string[];
  installedNpmPackages: boolean;
}

export async function init(
  projectDir: string,
  projectConfig: dataform.IProjectConfig,
  skipInstall?: boolean
): Promise<InitResult> {
  const dataformJsonPath = path.join(projectDir, "dataform.json");
  const packageJsonPath = path.join(projectDir, "package.json");
  const gitignorePath = path.join(projectDir, ".gitignore");
  if (fs.existsSync(dataformJsonPath) || fs.existsSync(packageJsonPath)) {
    throw new Error(
      "Cannot init dataform project, this already appears to be an NPM or Dataform directory."
    );
  }

  const filesWritten = [];
  const dirsCreated = [];

  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir);
    dirsCreated.push(projectDir);
  }

  fs.writeFileSync(
    dataformJsonPath,
    prettyJsonStringify(
      dataform.ProjectConfig.create({
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        ...projectConfig
      })
    )
  );
  filesWritten.push(dataformJsonPath);

  fs.writeFileSync(
    packageJsonPath,
    prettyJsonStringify({
      dependencies: {
        "@dataform/core": version
      }
    })
  );
  filesWritten.push(packageJsonPath);

  fs.writeFileSync(gitignorePath, "node_modules/\n");
  filesWritten.push(gitignorePath);

  // Make the default models, includes folders.
  const definitionsDir = path.join(projectDir, "definitions");
  fs.mkdirSync(definitionsDir);
  dirsCreated.push(definitionsDir);

  const includesDir = path.join(projectDir, "includes");
  fs.mkdirSync(includesDir);
  dirsCreated.push(includesDir);

  // Install packages.
  await install(projectDir, skipInstall);

  return {
    filesWritten,
    dirsCreated,
    installedNpmPackages: !skipInstall
  };
}

function prettyJsonStringify(obj) {
  return JSON.stringify(obj, null, 4) + "\n";
}
