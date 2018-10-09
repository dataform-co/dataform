import * as path from "path";
import * as fs from "fs";
import * as childProcess from "child_process";
import { promisify } from "util";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import install from "./install";

export default function init(projectDir: string, warehouse: string) {
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
        assertionSchema: "dataform"
      }),
      null,
      4
    ) + "\n"
  );
  fs.writeFileSync(
    packageJsonPath,
    JSON.stringify(
      {
        name: utils.baseFilename(path.resolve(projectDir)),
        version: "0.0.1",
        description: "New Dataform project.",
        dependencies: {
          "@dataform/core": "^0.0.2-alpha.7"
        }
      },
      null,
      4
    ) + "\n"
  );
  fs.writeFileSync(gitignorePath,
  `node_modules/
  `);
  // Make the default models, includes folders.
  fs.mkdirSync(path.join(projectDir, "models"));
  fs.mkdirSync(path.join(projectDir, "includes"));

  // Install packages.
  return install(projectDir);
}
