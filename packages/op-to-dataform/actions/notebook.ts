import { Session } from "df/core/session";
import { getCallerFile, resolvableAsActionConfigTarget } from "df/core/utils";
import * as Path from "df/core/path";
import { dataform } from "df/protos/ts";
import { IAction } from "../types";

export function transpileNotebook(action: IAction, session: Session, yamlPath?: string) {
  const notebookConfig = action.notebook;
  if (!notebookConfig) {
    return;
  }
  const name = notebookConfig.name;
  const filename = notebookConfig.mainFilePath;
  const dependsOn = notebookConfig.dependsOn || [];
  const stagingBucket = notebookConfig.stagingBucket;

  const isManagedSpark = !!notebookConfig.engine?.dataprocServerless;

  if (stagingBucket) {
    if (isManagedSpark) {
      if (!session.projectConfig.defaultManagedSparkExecutionOptions) {
        session.projectConfig.defaultManagedSparkExecutionOptions = {};
      }
      if (!session.projectConfig.defaultManagedSparkExecutionOptions.stagingBucketUri) {
        session.projectConfig.defaultManagedSparkExecutionOptions.stagingBucketUri = stagingBucket;
      }
    } else {
      if (!session.projectConfig.defaultNotebookRuntimeOptions) {
        session.projectConfig.defaultNotebookRuntimeOptions = {};
      }
      if (!session.projectConfig.defaultNotebookRuntimeOptions.outputBucket) {
        session.projectConfig.defaultNotebookRuntimeOptions.outputBucket = stagingBucket;
      }
    }
  }

  const normalizedNotebook = filename.replace(/\\/g, "/");
  const normalizedYaml = yamlPath ? yamlPath.replace(/\\/g, "/") : undefined;

  let projectRootRelativePath = normalizedNotebook;
  if (normalizedYaml) {
    const lastSlash = normalizedYaml.lastIndexOf("/");
    const yamlDir = lastSlash !== -1 ? normalizedYaml.substring(0, lastSlash) : "";
    if (yamlDir && !normalizedNotebook.startsWith("definitions/")) {
      projectRootRelativePath = `${yamlDir}/${normalizedNotebook}`;
    }
  }

  const callerFile = getCallerFile(session.rootDir);
  const callerDir = Path.dirName(callerFile);
  const resolvedPathForSession = getRelativePath(callerDir, projectRootRelativePath);

  session.notebook({
    name,
    filename: resolvedPathForSession,
    dependencyTargets: dependsOn.map(dep => resolvableAsActionConfigTarget(dep)),
    tags: action.tags || [],
    ...(isManagedSpark && {
      executionEngine: dataform.ActionConfig.NotebookConfig.ExecutionEngine.MANAGED_SPARK
    })
  } as any);
}

function getRelativePath(fromDir: string, toPath: string): string {
  const fromParts = fromDir.split("/").filter(p => !!p && p !== ".");
  const toParts = toPath.split("/").filter(p => !!p && p !== ".");

  let commonDepth = 0;
  while (
    commonDepth < fromParts.length &&
    commonDepth < toParts.length &&
    fromParts[commonDepth] === toParts[commonDepth]
  ) {
    commonDepth++;
  }

  const backOps = new Array(fromParts.length - commonDepth).fill("..");
  const forwardOps = toParts.slice(commonDepth);

  return [...backOps, ...forwardOps].join("/");
}
