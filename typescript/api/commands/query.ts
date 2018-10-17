import * as fs from "fs";
import * as util from "util";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import * as runners from "../runners";


export function run(profile: protos.IProfile, query: string, projectDir?: string): Promise<any[]> {
  var compiledQuery = compile(query, projectDir);
  return runners.create(profile).execute(compiledQuery);
}

export function compile(query: string, projectDir?: string) {
  var compiledQuery = query;
  if (projectDir) {
    const vm = new NodeVM({
      timeout: 5000,
      wrapper: "none",
      require: {
        context: "sandbox",
        root: projectDir,
        external: true
      },
      sourceExtensions: ["js"],
    });
    var indexScript = genQueryCompileIndex(projectDir, query);
    compiledQuery = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  }
  return compiledQuery;
}

function genQueryCompileIndex(projectDir: string, query: string): string {
  var packageJsonPath = path.join(projectDir, "package.json");
  var packageConfig = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  var includePaths = [];
  glob.sync("includes/*.js", { cwd: projectDir }).forEach(path => {
    if (includePaths.indexOf(path) < 0) {
      includePaths.push(path);
    }
  });

  var packageRequires = Object.keys(packageConfig.dependencies || {})
    .map(packageName => {
      return `global.${utils.variableNameFriendly(
        packageName
      )} = require("${packageName}");`;
    })
    .join("\n");

  var includeRequires = includePaths
    .map(path => {
      return `try { global.${utils.baseFilename(path)} = require("./${path}"); } catch (e) { throw Error("Exception in ${path}: " + e) }`;
    })
    .join("\n");

  return `
    const dataformcore = require("@dataform/core");
    dataformcore.Dataform.ROOT_DIR="${projectDir}";
    dataformcore.init(require("./dataform.json"));
    const ref = dataformcore.singleton.ref.bind(dataformcore.singleton);
    ${packageRequires}
    ${includeRequires}
    return \`${query}\``;
}
