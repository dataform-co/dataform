// tslint:disable tsr-detect-non-literal-fs-filename
import * as fs from "fs-extra";
import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

import { decode64, encode64 } from "df/common/protos";
import { compile } from "df/core/compilers";
import { dataform } from "df/protos/ts";

export const VALID_DATAFORM_JSON = `
{
  "defaultDatabase": "defaultProject",
  "defaultSchema": "defaultDataset",
  "defaultLocation": "US"
}
`;

export const VALID_WORKFLOW_SETTINGS_YAML = `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
`;

export class WorkflowSettingsTemplates {
  public static bigquery = dataform.WorkflowSettings.create({
    defaultDataset: "defaultDataset",
    defaultLocation: "US"
  });

  public static bigqueryWithDefaultProject = dataform.WorkflowSettings.create({
    ...WorkflowSettingsTemplates.bigquery,
    defaultProject: "defaultProject"
  });

  public static bigqueryWithDatasetSuffix = dataform.WorkflowSettings.create({
    ...WorkflowSettingsTemplates.bigquery,
    datasetSuffix: "suffix"
  });

  public static bigqueryWithDefaultProjectAndDataset = dataform.WorkflowSettings.create({
    ...WorkflowSettingsTemplates.bigqueryWithDefaultProject,
    projectSuffix: "suffix"
  });

  public static bigqueryWithNamePrefix = dataform.WorkflowSettings.create({
    ...WorkflowSettingsTemplates.bigquery,
    namePrefix: "prefix"
  });
}

const SOURCE_EXTENSIONS = ["js", "sql", "sqlx", "yaml", "yml", "ipynb"];

export function coreExecutionRequestFromPath(
  projectDir: string,
  projectConfigOverride?: dataform.ProjectConfig
): dataform.CoreExecutionRequest {
  const resolvedProjectDir = fs.realpathSync(path.resolve(projectDir));
  return dataform.CoreExecutionRequest.create({
    compile: {
      compileConfig: {
        projectDir: resolvedProjectDir,
        filePaths: walkDirectoryForFilenames(resolvedProjectDir),
        projectConfigOverride,
        extension: {
          name: "none",
          compilationMode: dataform.ExtensionCompilationMode.COMPILATION_MODE_UNSPECIFIED
        }
      }
    }
  });
}

// A VM is needed when running main because Node functions like `require` are overridden.
export function runMainInVm(
  coreExecutionRequest: dataform.CoreExecutionRequest
): dataform.CoreExecutionResponse {
  const projectDir = coreExecutionRequest.compile.compileConfig.projectDir;

  // Copy over the build Dataform Core that is set up as a node_modules directory.
  fs.copySync(`${process.cwd()}/core/node_modules`, `${projectDir}/node_modules`);

  const compiler = compile as CompilerFunction;
  // See cli/vm/compile.ts for why we use a host-side stack + enter/exit helpers
  // instead of writing to `global.__dataform_current_file` inside every module.
  const fileStack: string[] = [];

  // Then use vm2's native compiler integration to apply the compiler to files.
  const nodeVm = new NodeVM({
    // Inheriting the console makes console.logs show when tests are running, which is useful for
    // debugging.
    console: "inherit",
    wrapper: "none",
    sandbox: {
      __df_enter: (p: string) => { fileStack.push(p); },
      __df_exit: () => { fileStack.pop(); },
      __df_current: () => fileStack.length > 0 ? fileStack[fileStack.length - 1] : null
    },
    require: {
      builtin: ["path", "fs"],
      context: "sandbox",
      external: true,
      root: projectDir,
      resolve: (moduleName: string, parentDirName: string) => {
        if (moduleName === "path" || moduleName === "fs") {
          return moduleName;
        }
        return path.join(parentDirName, path.relative(parentDirName, projectDir), moduleName);
      }
    },
    sourceExtensions: SOURCE_EXTENSIONS,
    compiler: (code, filePath) => {
      const compiledCode = compiler(code, filePath);
      return `
        __df_enter(${JSON.stringify(filePath)});
        try {
          ${compiledCode}
        } finally {
          __df_exit();
        }
      `;
    }
  });

  const encodedCoreExecutionRequest = encode64(dataform.CoreExecutionRequest, coreExecutionRequest);
  const vmIndexFileName = path.resolve(path.join(projectDir, "index.js"));
  const encodedCoreExecutionResponse = nodeVm.run(
    `
      Object.defineProperty(global, '__dataform_current_file', {
        configurable: true,
        get: function() { return __df_current(); }
      });
      global.workflowSettingsYaml = (function() { try { return require("./workflow_settings.yaml"); } catch(e) { console.error("YAML require failed run_core:", e); } })();
      global.dataformJson = (function() { try { return require("./dataform.json"); } catch(e) {} })();
      return require("@dataform/core").main("${encodedCoreExecutionRequest}")
    `,
    vmIndexFileName
  );
  return decode64(dataform.CoreExecutionResponse, encodedCoreExecutionResponse);
}

function walkDirectoryForFilenames(projectDir: string, relativePath: string = ""): string[] {
  let paths: string[] = [];
  fs.readdirSync(path.join(projectDir, relativePath), { withFileTypes: true })
    .filter(directoryEntry => directoryEntry.name !== "node_modules")
    .forEach(directoryEntry => {
      const entryRelativePath = path.join(relativePath, directoryEntry.name);
      if (directoryEntry.isDirectory()) {
        paths = paths.concat(walkDirectoryForFilenames(projectDir, entryRelativePath));
        return;
      }
      const fileExtension = directoryEntry.name.split(".").slice(-1)[0];
      if (directoryEntry.isFile() && SOURCE_EXTENSIONS.includes(fileExtension)) {
        paths.push(entryRelativePath);
      }
    });
  return paths;
}
