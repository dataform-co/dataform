import { dataform } from "@dataform/protos";
import * as glob from "glob";
import { util } from "protobufjs";

export function createGenIndexConfig(
  compileConfig: dataform.ICompileConfig,
  returnOverride?: string
): string {
  const includePaths = [];
  glob.sync("includes/*.js", { cwd: compileConfig.projectDir }).forEach(path => {
    if (includePaths.indexOf(path) < 0) {
      includePaths.push(path);
    }
  });

  const definitionPaths = [];
  glob.sync("definitions/**/*.{js,sql,sqlx}", { cwd: compileConfig.projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });
  // Support projects that don't use the new project structure.
  glob.sync("models/**/*.{js,sql}", { cwd: compileConfig.projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });
  const encodedConfigBytes = dataform.GenerateIndexConfig.encode({
    compileConfig,
    includePaths,
    definitionPaths,
    returnOverride
  }).finish();
  return util.base64.encode(encodedConfigBytes, 0, encodedConfigBytes.length);
}
