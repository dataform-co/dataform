import * as glob from "glob";
import { util } from "protobufjs";

import { dataform } from "df/protos/ts";
import { encode } from "df/common/protos";

export function createGenIndexConfig(compileConfig: dataform.ICompileConfig): string {
  const includePaths: string[] = [];
  glob.sync("includes/*.js", { cwd: compileConfig.projectDir }).forEach(path => {
    if (includePaths.indexOf(path) < 0) {
      includePaths.push(path);
    }
  });

  const definitionPaths: string[] = [];
  glob.sync("definitions/**/*.{js,sql,sqlx}", { cwd: compileConfig.projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });
  // Support projects that don't use the new project structure.
  glob.sync("models/**/*.{js,sql,sqlx}", { cwd: compileConfig.projectDir }).forEach(path => {
    if (definitionPaths.indexOf(path) < 0) {
      definitionPaths.push(path);
    }
  });
  return encode(dataform.GenerateIndexConfig, {
    compileConfig,
    includePaths,
    definitionPaths,
    // For backwards compatibility with old versions of @dataform/core.
    returnOverride: compileConfig.returnOverride
  });
}
