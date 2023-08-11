import * as fs from "fs";
import * as path from "path";

import { CREDENTIALS_FILENAME } from "df/api/commands/credentials";
import { install } from "df/api/commands/install";
import { prettyJsonStringify } from "df/api/utils";
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
  includeSchedules?: boolean;
  includeEnvironments?: boolean;
}

export async function init(
  projectDir: string,
  projectConfig: dataform.IProjectConfig,
  options: IInitOptions = {}
): Promise<IInitResult> {
  const dataformJsonPath = path.join(projectDir, "dataform.json");
  const packageJsonPath = path.join(projectDir, "package.json");
  const gitignorePath = path.join(projectDir, ".gitignore");
  const schedulesJsonPath = path.join(projectDir, "schedules.json");
  const environmentsJsonPath = path.join(projectDir, "environments.json");

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

  fs.writeFileSync(gitignorePath, gitIgnoreContents);
  filesWritten.push(gitignorePath);

  if (options.includeSchedules) {
    fs.writeFileSync(
      schedulesJsonPath,
      prettyJsonStringify(dataform.schedules.SchedulesJSON.create({}))
    );
    filesWritten.push(schedulesJsonPath);
  }

  if (options.includeEnvironments) {
    fs.writeFileSync(environmentsJsonPath, prettyJsonStringify(dataform.Environments.create({})));
  }

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
