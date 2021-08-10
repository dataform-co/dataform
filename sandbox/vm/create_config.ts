import * as glob from "glob";

import { encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

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
  return encode64(dataform.GenerateIndexConfig, {
    compileConfig,
    includePaths,
    definitionPaths,
    // For backwards compatibility with old versions of @dataform/core.
    returnOverride: compileConfig.returnOverride
  });
}

/**
 * @returns a base64 encoded {@see dataform.CoreExecutionConfig} proto.
 */
export function createCoreExecutionConfig(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }))
  );

  return encode64(dataform.CoreExecutionConfig, {
    // Add the list of file paths to the compile config if not already set.
    compileConfig: { filePaths, ...compileConfig }
  });
}
