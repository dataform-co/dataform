import * as glob from "glob";

import { encode64 } from "df/common/protos";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export function createGenIndexConfig(compileConfig: dataform.CompileConfig): string {
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
    definitionPaths
  });
}

/**
 * @returns a base64 encoded {@see dataform.CoreExecutionRequest} proto.
 */
export function createCoreExecutionRequest(compileConfig: dataform.CompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }))
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } }
  });
}
