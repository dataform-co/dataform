import { session as defaultSession } from "df/core";
import { Session } from "df/core/session";
import * as $protobuf from "protobufjs";
import { IAction, IPipeline } from "./types";
import { transpileSql } from "./actions/sql";
import { transpileNotebook } from "./actions/notebook";
import { transpileAirflowOperator } from "./actions/airflow_operator";

export * from "./types";

function registerStructWrappers() {
  $protobuf.wrappers[".google.protobuf.Struct"] = {
    fromObject(object: any) {
      if (object && object.fields) {
        return this.create(object);
      }
      return this.create(structFromObject(object));
    },
    toObject(message: any, options: any) {
      return structToObject(message);
    }
  } as any;

  $protobuf.wrappers[".google.protobuf.Value"] = {
    fromObject(object: any) {
      if (object && (
        object.nullValue !== undefined ||
        object.numberValue !== undefined ||
        object.stringValue !== undefined ||
        object.boolValue !== undefined ||
        object.structValue !== undefined ||
        object.listValue !== undefined
      )) {
        return this.create(object);
      }
      return this.create(valueFromObject(object));
    },
    toObject(message: any, options: any) {
      return valueToObject(message);
    }
  } as any;

  $protobuf.wrappers[".google.protobuf.ListValue"] = {
    fromObject(object: any) {
      if (object && object.values) {
        return this.create(object);
      }
      return this.create({ values: object.map(valueFromObject) });
    },
    toObject(message: any, options: any) {
      return listValueToObject(message);
    }
  } as any;
}

function valueFromObject(value: any): any {
  if (value === null || value === undefined) {
    return { nullValue: 0 };
  }
  if (typeof value === "number") {
    return { numberValue: value };
  }
  if (typeof value === "string") {
    return { stringValue: value };
  }
  if (typeof value === "boolean") {
    return { boolValue: value };
  }
  if (Array.isArray(value)) {
    return { listValue: { values: value.map(valueFromObject) } };
  }
  if (typeof value === "object") {
    return { structValue: structFromObject(value) };
  }
  return {};
}

function structFromObject(object: any): any {
  const fields: any = {};
  for (const key of Object.keys(object)) {
    fields[key] = valueFromObject(object[key]);
  }
  return { fields };
}

function structToObject(message: any): any {
  const obj: any = {};
  const fields = message.fields || {};
  for (const key of Object.keys(fields)) {
    obj[key] = valueToObject(fields[key]);
  }
  return obj;
}

function valueToObject(message: any): any {
  if (message.nullValue !== undefined && message.nullValue !== null) {
    return null;
  }
  if (message.numberValue !== undefined && message.numberValue !== null) {
    return message.numberValue;
  }
  if (message.stringValue !== undefined && message.stringValue !== null) {
    return message.stringValue;
  }
  if (message.boolValue !== undefined && message.boolValue !== null) {
    return message.boolValue;
  }
  if (message.structValue !== undefined && message.structValue !== null) {
    return structToObject(message.structValue);
  }
  if (message.listValue !== undefined && message.listValue !== null) {
    return listValueToObject(message.listValue);
  }
  return undefined;
}

function listValueToObject(message: any): any {
  return (message.values || []).map(valueToObject);
}

function loadOrchestrationPipelineSchema(): $protobuf.Type {
  registerStructWrappers();
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

export function transpileAction(action: IAction, session: Session = defaultSession, yamlPath?: string, runner?: string) {
  if (action.sql) {
    transpileSql(action, session, yamlPath);
  } else if (action.notebook) {
    transpileNotebook(action, session, yamlPath);
  } else if (action.airflowOperator) {
    transpileAirflowOperator(action, session, yamlPath, runner);
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

  configureSessionDefaults(pipelineMessage, session);

  const runnerEnum = OrchestrationPipeline.lookupEnum("PipelineRunner");
  const runnerName = runnerEnum.valuesById[pipelineMessage.runner];

  const actions = pipelineMessage.actions || [];
  resolveDependencies(actions);
  actions.forEach((action: any) => transpileAction(action, session, yamlPath, runnerName));
}

function configureSessionDefaults(pipelineMessage: any, session: Session) {
  if (pipelineMessage.defaults) {
    const { projectId, location, stagingBucket, runtimeTemplateName } = pipelineMessage.defaults;
    if (projectId) {
      if (!session.projectConfig.defaultDatabase) {
        session.projectConfig.defaultDatabase = projectId;
      }
      if (!session.canonicalProjectConfig.defaultDatabase) {
        session.canonicalProjectConfig.defaultDatabase = projectId;
      }
    }
    if (location) {
      if (!session.projectConfig.defaultLocation) {
        session.projectConfig.defaultLocation = location;
      }
      if (!session.canonicalProjectConfig.defaultLocation) {
        session.canonicalProjectConfig.defaultLocation = location;
      }
    }
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
    if (runtimeTemplateName) {
      if (!session.projectConfig.defaultNotebookRuntimeOptions) {
        session.projectConfig.defaultNotebookRuntimeOptions = {};
      }
      if (!session.projectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName) {
        session.projectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName = runtimeTemplateName;
      }
      if (!session.canonicalProjectConfig.defaultNotebookRuntimeOptions) {
        session.canonicalProjectConfig.defaultNotebookRuntimeOptions = {};
      }
      if (!session.canonicalProjectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName) {
        session.canonicalProjectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName = runtimeTemplateName;
      }
    }
  }
  if (!session.projectConfig.warehouse) {
    session.projectConfig.warehouse = "bigquery";
  }
  if (!session.canonicalProjectConfig.warehouse) {
    session.canonicalProjectConfig.warehouse = "bigquery";
  }
  if (!session.projectConfig.defaultManagedSparkExecutionOptions?.stagingBucketUri) {
    const actions = pipelineMessage.actions || [];
    for (const action of actions) {
      const stagingBucket = action.notebook?.stagingBucket || action.pyspark?.stagingBucket;
      if (stagingBucket) {
        if (!session.projectConfig.defaultManagedSparkExecutionOptions) {
          session.projectConfig.defaultManagedSparkExecutionOptions = {};
        }
        session.projectConfig.defaultManagedSparkExecutionOptions.stagingBucketUri = stagingBucket;
        if (!session.canonicalProjectConfig.defaultManagedSparkExecutionOptions) {
          session.canonicalProjectConfig.defaultManagedSparkExecutionOptions = {};
        }
        session.canonicalProjectConfig.defaultManagedSparkExecutionOptions.stagingBucketUri = stagingBucket;
        break;
      }
    }
  }
  if (!session.projectConfig.defaultNotebookRuntimeOptions?.runtimeTemplateName) {
    const actions = pipelineMessage.actions || [];
    for (const action of actions) {
      const runtimeTemplateName =
        action.notebook?.engine?.dataprocOnGce?.ephemeralCluster?.properties?.runtimeTemplateName;
      if (runtimeTemplateName) {
        if (!session.projectConfig.defaultNotebookRuntimeOptions) {
          session.projectConfig.defaultNotebookRuntimeOptions = {};
        }
        session.projectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName = runtimeTemplateName;
        if (!session.canonicalProjectConfig.defaultNotebookRuntimeOptions) {
          session.canonicalProjectConfig.defaultNotebookRuntimeOptions = {};
        }
        session.canonicalProjectConfig.defaultNotebookRuntimeOptions.runtimeTemplateName = runtimeTemplateName;
        break;
      }
    }
  }
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
    } else if (action.airflowOperator) {
      const name = action.airflowOperator.name;
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
    } else if (action.airflowOperator && action.airflowOperator.dependsOn) {
      action.airflowOperator.dependsOn = action.airflowOperator.dependsOn.map((dep: string) => {
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
          const normalizedVal = val.replace(/-/g, "_");
          const numVal = field.resolvedType.values[normalizedVal];
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
