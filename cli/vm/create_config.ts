import * as glob from "glob";

import { encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export function createGenIndexConfig(compileConfig: dataform.ICompileConfig): string {
  const includePaths: string[] = [];
  glob
    .sync("includes/*.js", { cwd: compileConfig.projectDir })
    .sort(alphabetically)
    .forEach(path => {
      if (includePaths.indexOf(path) < 0) {
        includePaths.push(path);
      }
    });

  const definitionPaths: string[] = [];
  glob
    .sync("definitions/**/*.{js,sql,sqlx}", { cwd: compileConfig.projectDir })
    .sort(alphabetically)
    .forEach(path => {
      if (definitionPaths.indexOf(path) < 0) {
        definitionPaths.push(path);
      }
    });
  // Support projects that don't use the new project structure.
  glob
    .sync("models/**/*.{js,sql,sqlx}", { cwd: compileConfig.projectDir })
    .sort(alphabetically)
    .forEach(path => {
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
export function createCoreExecutionRequest(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(
      glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }).sort(alphabetically)
    )
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } }
  });
}

// NOTE: this is used to sort results of `glob.sync()` above.
// This sort is only required for compatibility with @dataform/core <= 2.6.5.
// If/when the CLI drops support for those versions, we can remove the sorting.
const alphabetically = (a: string, b: string) => a.localeCompare(b);
