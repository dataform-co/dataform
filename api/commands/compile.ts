import * as fs from "fs";
import * as path from "path";
import * as semver from "semver";
import * as glob from "glob";

import deepmerge from "deepmerge";
import { validWarehouses } from "df/api/dbadapters";
import { ErrorWithCause } from "df/common/errors/errors";
import { encode64, decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { main as coreCompile } from "df/core";

// Project config properties that are required.
const mandatoryProps: Array<keyof dataform.IProjectConfig> = ["warehouse", "defaultSchema"];

// Project config properties that require alphanumeric characters, hyphens or underscores.
const simpleCheckProps: Array<keyof dataform.IProjectConfig> = [
  "assertionSchema",
  "databaseSuffix",
  "schemaSuffix",
  "tablePrefix",
  "defaultSchema"
];

export class CompilationTimeoutError extends Error {}

export async function compile(
  compileConfig: dataform.ICompileConfig = {}
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    const projectConfig = JSON.parse(dataformJson);
    checkDataformJsonValidity(deepmerge(projectConfig, compileConfig.projectConfigOverride || {}));
  } catch (e) {
    throw new ErrorWithCause(
      `Compilation failed. ProjectConfig ('dataform.json') is invalid: ${e.message}`,
      e
    );
  }

  if (compileConfig.useMain === null || compileConfig.useMain === undefined) {
    try {
      const packageJson = JSON.parse(
        fs.readFileSync(`${compileConfig.projectDir}/package.json`, "utf8")
      );
      const dataformCoreVersion = packageJson.dependencies["@dataform/core"];
      compileConfig.useMain = semver.subset(dataformCoreVersion, ">=2.0.4");
    } catch (e) {
      // Silently catch any thrown Error. Do not attempt to use `main` compilation.
    }
  }

  const coreExecutionRequest = createCoreExecutionRequest(compileConfig);
  const result = coreCompile(coreExecutionRequest);

  if (compileConfig.useMain) {
    const decodedResult = decode64(dataform.CoreExecutionResponse, result);
    return dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);
  }

  return decode64(dataform.CompiledGraph, result);
}

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
    definitionPaths
  });
}

/**
 * @returns a base64 encoded {@see dataform.CoreExecutionRequest} proto.
 */
export function createCoreExecutionRequest(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }))
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } }
  });
}

export const checkDataformJsonValidity = (dataformJsonParsed: { [prop: string]: any }) => {
  const invalidWarehouseProp = () => {
    return dataformJsonParsed.warehouse && !validWarehouses.includes(dataformJsonParsed.warehouse)
      ? `Invalid value on property warehouse: ${
          dataformJsonParsed.warehouse
        }. Should be one of: ${validWarehouses.join(", ")}.`
      : null;
  };
  const invalidProp = () => {
    const invProp = simpleCheckProps.find(prop => {
      return prop in dataformJsonParsed && !/^[a-zA-Z_0-9\-]*$/.test(dataformJsonParsed[prop]);
    });
    return invProp
      ? `Invalid value on property ${invProp}: ${dataformJsonParsed[invProp]}. Should only contain alphanumeric characters, underscores and/or hyphens.`
      : null;
  };
  const missingMandatoryProp = () => {
    const missMandatoryProp = mandatoryProps.find(prop => {
      return !(prop in dataformJsonParsed);
    });
    return missMandatoryProp ? `Missing mandatory property: ${missMandatoryProp}.` : null;
  };
  const message = invalidWarehouseProp() || invalidProp() || missingMandatoryProp();
  if (message) {
    throw new Error(message);
  }
};
