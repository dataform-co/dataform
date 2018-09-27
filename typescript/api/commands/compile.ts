import * as fs from "fs";
import * as util from "util";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";

export default function compile(projectDir: string): protos.ICompiledGraph {
  const vm = new NodeVM({
    timeout: 5000,
    wrapper: "none",
    require: {
      context: "sandbox",
      root: projectDir,
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: (code, file) => {
      if (file.includes(".sql")) {
        return utils.compileSql(code, file);
      } else {
        return code;
      }
    }
  });
  var indexScript = genCompileIndex(projectDir);
  return vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
}

function genCompileIndex(projectDir: string): string {
  var projectConfig = protos.ProjectConfig.create({
    datasetPaths: ["datasets/*"],
    includePaths: ["includes/*"]
  });

  var projectConfigPath = path.join(projectDir, "dataform.json");
  if (fs.existsSync(projectConfigPath)) {
    Object.assign(
      projectConfig,
      JSON.parse(fs.readFileSync(projectConfigPath, "utf8"))
    );
  }

  var packageJsonPath = path.join(projectDir, "package.json");
  var packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  var includePaths = [];
  projectConfig.includePaths.forEach(pathPattern =>
    glob.sync(pathPattern, { cwd: projectDir }).forEach(path => {
      if (includePaths.indexOf(path) < 0) {
        includePaths.push(path);
      }
    })
  );
  var datasetPaths = [];
  projectConfig.datasetPaths.forEach(pathPattern =>
    glob.sync(pathPattern, { cwd: projectDir }).forEach(path => {
      if (datasetPaths.indexOf(path) < 0) {
        datasetPaths.push(path);
      }
    })
  );

  var packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `global.${utils.variableNameFriendly(
        packageName
      )} = require("${packageName}");`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `global.${utils.baseFilename(path)} = require("./${path}");`;
    })
    .join("\n");
  var datasetRequires = datasetPaths
    .map(path => {
      return `require("./${path}");`;
    })
    .join("\n");

  return `
    const dataformcore = require("@dataform/core");
    dataformcore.Dataform.ROOT_DIR="${projectDir}";
    dataformcore.init(require("./dataform.json"));
    ${packageRequires}
    ${includeRequires}
    ${datasetRequires}
    return dataformcore.compile();`;
}
