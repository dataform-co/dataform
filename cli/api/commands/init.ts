import * as fs from "fs";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { CREDENTIALS_FILENAME } from "df/cli/api/commands/credentials";
import { install } from "df/cli/api/commands/install";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

const gitIgnoreContents = `
${CREDENTIALS_FILENAME}
node_modules/
`;

export interface IInitResult {
  filesWritten: string[];
  dirsCreated: string[];
  installedNpmPackages: boolean;
}

export interface IInitOptions {
  skipInstall?: boolean;
}

export async function init(
  projectDir: string,
  projectConfig: dataform.IProjectConfig,
  options: IInitOptions = {}
): Promise<IInitResult> {
  const workflowSettingsYamlPath = path.join(projectDir, "workflow_settings.yaml");
  const packageJsonPath = path.join(projectDir, "package.json");
  const gitignorePath = path.join(projectDir, ".gitignore");
  // dataform.json is Deprecated.
  const dataformJsonPath = path.join(projectDir, "dataform.json");

  if (
    fs.existsSync(workflowSettingsYamlPath) ||
    fs.existsSync(packageJsonPath) ||
    fs.existsSync(dataformJsonPath)
  ) {
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

  const workflowSettings = dataform.WorkflowSettings.create({
    defaultDataset: projectConfig.defaultSchema || "dataform",
    defaultAssertionDataset: projectConfig.assertionSchema || "dataform_assertions",
    dataformCoreVersion: version
  });
  if (projectConfig.defaultDatabase) {
    workflowSettings.defaultProject = projectConfig.defaultDatabase;
  }
  if (projectConfig.defaultLocation) {
    workflowSettings.defaultLocation = projectConfig.defaultLocation;
  }
  if (projectConfig.vars) {
    workflowSettings.vars = projectConfig.vars;
  }
  if (projectConfig.databaseSuffix) {
    workflowSettings.projectSuffix = projectConfig.databaseSuffix;
  }
  if (projectConfig.schemaSuffix) {
    workflowSettings.datasetSuffix = projectConfig.schemaSuffix;
  }
  if (projectConfig.tablePrefix) {
    workflowSettings.namePrefix = projectConfig.tablePrefix;
  }

  fs.writeFileSync(workflowSettingsYamlPath, dumpYaml(workflowSettings));
  filesWritten.push(workflowSettingsYamlPath);

  fs.writeFileSync(gitignorePath, gitIgnoreContents);
  filesWritten.push(gitignorePath);

  // Make the default models, includes folders.
  const definitionsDir = path.join(projectDir, "definitions");
  fs.mkdirSync(definitionsDir);
  dirsCreated.push(definitionsDir);

  const includesDir = path.join(projectDir, "includes");
  fs.mkdirSync(includesDir);
  dirsCreated.push(includesDir);

  // Install packages.
  await install(projectDir, options.skipInstall);

  return {
    filesWritten,
    dirsCreated,
    installedNpmPackages: !options.skipInstall
  };
}
