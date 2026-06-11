import { session as defaultSession } from "df/core";
import { Session } from "df/core/session";
import * as $protobuf from "protobufjs";
import { IAction, IPipeline } from "./types";
import { transpileSql } from "./actions/sql";
import { transpileNotebook } from "./actions/notebook";

export * from "./types";

function loadOrchestrationPipelineSchema(): $protobuf.Type {
  const root = new $protobuf.Root();

  // Load google/protobuf/struct.proto
  const structCommon = $protobuf.common.get("google/protobuf/struct.proto");
  if (structCommon) {
    $protobuf.Root.fromJSON(structCommon, root);
  }

  // Parse descriptor.proto (needed for FieldOptions extensions in validation.proto)
  const descriptorProto = require("protobufjs/google/protobuf/descriptor.proto");
  $protobuf.parse(descriptorProto, root);

  // Parse validation.proto
  const validationProto = require("./protos/validation.proto");
  $protobuf.parse(validationProto, root);

  // Parse orchestration_pipeline.proto
  const orchestrationPipelineProto = require("./protos/orchestration_pipeline.proto");
  $protobuf.parse(orchestrationPipelineProto, root);

  root.resolveAll();
  return root.lookupType("pipeline_models.OrchestrationPipeline");
}

const OrchestrationPipeline = loadOrchestrationPipelineSchema();

export function transpileAction(action: IAction, session: Session = defaultSession, yamlPath?: string) {
  if (action.sql) {
    transpileSql(action, session, yamlPath);
  } else if (action.notebook) {
    transpileNotebook(action, session, yamlPath);
  }
}

export function transpilePipeline(pipeline: IPipeline, session: Session = defaultSession, yamlPath?: string) {
  const unknownFieldsErrors = checkUnknownFields(pipeline, OrchestrationPipeline);
  if (unknownFieldsErrors.length > 0) {
    throw new Error(`Pipeline validation failed: ${unknownFieldsErrors.join(", ")}`);
  }

  resolveEnumStrings(pipeline, OrchestrationPipeline);
  const validationError = OrchestrationPipeline.verify(pipeline);
  if (validationError) {
    throw new Error(`Pipeline validation failed: ${validationError}`);
  }

  const pipelineMessage = OrchestrationPipeline.fromObject(pipeline) as any;
  const actions = pipelineMessage.actions || [];
  resolveDependencies(actions);
  actions.forEach((action: any) => transpileAction(action, session, yamlPath));
}

function resolveDependencies(actions: any[]) {
  const logicalToResolved = new Map<string, any>();
  for (const action of actions) {
    if (action.sql) {
      const name = action.sql.name;
      const bq = action.sql.engine?.bigquery;
      if (bq && bq.destinationTable) {
        const parts = bq.destinationTable.split(".");
        const tableName = parts[parts.length - 1];
        const dataset = parts.length > 1 ? parts[parts.length - 2] : undefined;
        const project = parts.length > 2 ? parts[parts.length - 3] : undefined;
        const target: any = { name: tableName };
        if (dataset) {
          target.dataset = dataset;
        }
        if (project) {
          target.project = project;
        }
        logicalToResolved.set(name, target);
      } else {
        logicalToResolved.set(name, { name });
      }
    } else if (action.notebook) {
      const name = action.notebook.name;
      logicalToResolved.set(name, { name });
    }
  }

  for (const action of actions) {
    if (action.sql && action.sql.dependsOn) {
      action.sql.dependsOn = action.sql.dependsOn.map((dep: string) => {
        const resolved = logicalToResolved.get(dep);
        return resolved ? resolved : dep;
      });
    } else if (action.notebook && action.notebook.dependsOn) {
      action.notebook.dependsOn = action.notebook.dependsOn.map((dep: string) => {
        const resolved = logicalToResolved.get(dep);
        return resolved ? resolved : dep;
      });
    }
  }
}

function checkUnknownFields(obj: any, type: any, path: string = ""): string[] {
  if (!obj || typeof obj !== "object") return [];
  if (
    type.fullName === ".google.protobuf.Struct" ||
    type.fullName === ".google.protobuf.Value" ||
    type.fullName === ".google.protobuf.ListValue"
  ) {
    return [];
  }
  const errors: string[] = [];

  for (const fieldName of Object.keys(obj)) {
    const currentPath = path ? `${path}.${fieldName}` : fieldName;
    const field = type.fields[fieldName];
    if (!field) {
      errors.push(`Ignored/unknown field: "${currentPath}"`);
      continue;
    }

    const val = obj[fieldName];
    if (val === null || val === undefined) continue;

    if (field.map) {
      if (field.resolvedType && typeof val === "object") {
        for (const key of Object.keys(val)) {
          errors.push(...checkUnknownFields(val[key], field.resolvedType, `${currentPath}["${key}"]`));
        }
      }
    } else if (field.repeated) {
      if (Array.isArray(val)) {
        val.forEach((item: any, index: number) => {
          if (field.resolvedType) {
            errors.push(...checkUnknownFields(item, field.resolvedType, `${currentPath}[${index}]`));
          }
        });
      }
    } else if (field.resolvedType && !field.resolvedType.values) {
      errors.push(...checkUnknownFields(val, field.resolvedType, currentPath));
    }
  }
  return errors;
}

function resolveEnumStrings(obj: any, type: any) {
  if (!obj || typeof obj !== "object") return;

  for (const fieldName of Object.keys(obj)) {
    const field = type.fields[fieldName];
    if (!field) continue;

    const val = obj[fieldName];
    if (val === null || val === undefined) continue;

    if (field.repeated) {
      if (Array.isArray(val)) {
        val.forEach((item: any) => {
          if (field.resolvedType) {
            resolveEnumStrings(item, field.resolvedType);
          }
        });
      }
    } else if (field.resolvedType) {
      if (field.resolvedType.values) {
        // It's an Enum!
        if (typeof val === "string") {
          const numVal = field.resolvedType.values[val];
          if (numVal !== undefined) {
            obj[fieldName] = numVal;
          }
        }
      } else {
        // It's a Message Type!
        resolveEnumStrings(val, field.resolvedType);
      }
    }
  }
}
