import { Session } from "df/core/session";
import { resolvableAsActionConfigTarget, nativeRequire } from "df/core/utils";
import { IAction } from "../types";

export function transpileAirflowOperator(action: IAction, session: Session, yamlPath?: string) {
  const config = action.airflowOperator;
  if (!config) {
    return;
  }
  const name = config.name;
  const dependsOn = config.dependsOn || [];

  const normalizedYaml = yamlPath ? yamlPath.replace(/\\/g, "/") : "definitions/file.yaml";
  const lastSlash = normalizedYaml.lastIndexOf("/");
  const yamlDir = lastSlash !== -1 ? normalizedYaml.substring(0, lastSlash) : "definitions";

  const targetNotebookPath = "@dataform/op-to-dataform/notebooks/run_airflow_operator.ipynb";
  const resolvedPathForSession = getRelativePath(yamlDir, targetNotebookPath);

  const stagingBucket = config.stagingBucket;
  if (stagingBucket) {
    if (!session.projectConfig.defaultNotebookRuntimeOptions) {
      session.projectConfig.defaultNotebookRuntimeOptions = {};
    }
    if (!session.projectConfig.defaultNotebookRuntimeOptions.outputBucket) {
      session.projectConfig.defaultNotebookRuntimeOptions.outputBucket = stagingBucket;
    }
    if (!session.canonicalProjectConfig.defaultNotebookRuntimeOptions) {
      session.canonicalProjectConfig.defaultNotebookRuntimeOptions = {};
    }
    if (!session.canonicalProjectConfig.defaultNotebookRuntimeOptions.outputBucket) {
      session.canonicalProjectConfig.defaultNotebookRuntimeOptions.outputBucket = stagingBucket;
    }
  }

  const notebookInstance = session.notebook({
    name,
    filename: resolvedPathForSession,
    dependencyTargets: dependsOn.map(dep => resolvableAsActionConfigTarget(dep)),
    tags: action.tags || []
  } as any);

  // Generate Airflow Python DAG code: single DAG with a single task - use the operator class name and pass parameters.
  const parts = config.operatorClass.split(".");
  const className = parts.pop();
  const moduleName = parts.join(".");

  const plainParams = structToJS(config.params);
  const paramLines = Object.entries(plainParams || {}).map(([key, val]) => {
    return `        ${key}=${toPythonLiteral(val)},`;
  });

  const dagCode = `import datetime
from airflow import DAG
from ${moduleName} import ${className}

with DAG(
    dag_id="my-serverless-dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
) as dag:
    ${className}(
        task_id="my-serverless-task",
${paramLines.join("\n")}
    )
`;

  const base64DagCode = base64Encode(dagCode);

  let notebookJson: any = null;
  try {
    notebookJson = nativeRequire(targetNotebookPath).asJson;
  } catch (e) {
    console.warn(`[op-to-dataform] Warning: Could not resolve notebook ${targetNotebookPath} in require cache.`);
  }

  if (notebookJson) {
    let modified = false;
    for (const cell of notebookJson.cells) {
      if (cell.cell_type === "code" && cell.source) {
        const sourceStr = Array.isArray(cell.source) ? cell.source.join("") : cell.source;
        if (sourceStr.includes("SERVERLESS_DAG_FILE")) {
          const regex = /("name":\s*"SERVERLESS_DAG_FILE",\s*\n?\s*"value":\s*")[^"]*(")/;
          const updatedSourceStr = sourceStr.replace(regex, `$1${base64DagCode}$2`);
          cell.source = [updatedSourceStr];
          modified = true;
        }
      }
    }
    if (modified) {
      notebookInstance.ipynb(notebookJson);
    }
  }
}

function structToJS(struct: any): any {
  if (!struct) return {};
  if (!struct.fields) {
    return struct;
  }
  const obj: any = {};
  for (const key of Object.keys(struct.fields)) {
    obj[key] = valueToJS(struct.fields[key]);
  }
  return obj;
}

function valueToJS(val: any): any {
  if (!val) return null;
  if (val.nullValue !== undefined && val.nullValue !== null) {
    return null;
  }
  if (val.numberValue !== undefined && val.numberValue !== null) {
    return val.numberValue;
  }
  if (val.stringValue !== undefined && val.stringValue !== null) {
    return val.stringValue;
  }
  if (val.boolValue !== undefined && val.boolValue !== null) {
    return val.boolValue;
  }
  if (val.structValue !== undefined && val.structValue !== null) {
    return structToJS(val.structValue);
  }
  if (val.listValue !== undefined && val.listValue !== null) {
    return (val.listValue.values || []).map(valueToJS);
  }
  return val;
}

function toPythonLiteral(val: any): string {
  if (val === null || val === undefined) {
    return "None";
  }
  if (typeof val === "boolean") {
    return val ? "True" : "False";
  }
  if (typeof val === "number") {
    return val.toString();
  }
  if (typeof val === "string") {
    return JSON.stringify(val);
  }
  if (Array.isArray(val)) {
    return "[" + val.map(toPythonLiteral).join(", ") + "]";
  }
  if (typeof val === "object") {
    const items = Object.entries(val).map(([k, v]) => `${JSON.stringify(k)}: ${toPythonLiteral(v)}`);
    return "{" + items.join(", ") + "}";
  }
  return JSON.stringify(val);
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

function base64Encode(str: string): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const bytes: number[] = [];
  for (let i = 0; i < str.length; i++) {
    let code = str.charCodeAt(i);
    if (code < 0x80) {
      bytes.push(code);
    } else if (code < 0x800) {
      bytes.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
    } else if (code < 0xd800 || code >= 0xe000) {
      bytes.push(0xe0 | (code >> 12), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
    } else {
      i++;
      code = 0x10000 + (((code & 0x3ff) << 10) | (str.charCodeAt(i) & 0x3ff));
      bytes.push(
        0xf0 | (code >> 18),
        0x80 | ((code >> 12) & 0x3f),
        0x80 | ((code >> 6) & 0x3f),
        0x80 | (code & 0x3f)
      );
    }
  }

  let result = "";
  let i = 0;
  const len = bytes.length;
  for (; i < len - 2; i += 3) {
    result += chars[bytes[i] >> 2];
    result += chars[((bytes[i] & 3) << 4) | (bytes[i + 1] >> 4)];
    result += chars[((bytes[i + 1] & 15) << 2) | (bytes[i + 2] >> 6)];
    result += chars[bytes[i + 2] & 63];
  }
  if (i === len - 2) {
    result += chars[bytes[i] >> 2];
    result += chars[((bytes[i] & 3) << 4) | (bytes[i + 1] >> 4)];
    result += chars[(bytes[i + 1] & 15) << 2];
    result += "=";
  } else if (i === len - 1) {
    result += chars[bytes[i] >> 2];
    result += chars[(bytes[i] & 3) << 4];
    result += "==";
  }
  return result;
}
