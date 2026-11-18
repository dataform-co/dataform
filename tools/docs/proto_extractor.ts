import * as path from "path";
import * as protobuf from "protobufjs";

import { DEFAULT_PROJECT_CONFIG } from "df/core/session";

// Canonical `.proto` files shipped in the published `@dataform/core` npm package.
// Verified against the built package tarball in `core/readme_test.ts`.
export const BUNDLED_PROTO_FILES = [
  "core.proto",
  "configs.proto",
  "property_graph_config.proto",
  "db_adapter.proto",
  "jit.proto",
  "extension.proto",
] as const;

export interface IWorkflowSettingsFieldDoc {
  name: string;
  typeMarkdown: string;
  defaultMarkdown: string;
  descriptionMarkdown: string;
}

export interface IBundledProtoFileDoc {
  fileName: string;
  itemsMarkdown: string[];
}

const SCALAR_TYPE_LABELS: Record<string, string> = {
  bool: "boolean",
  int32: "number",
  int64: "number",
  uint32: "number",
  uint64: "number",
  sint32: "number",
  sint64: "number",
  fixed32: "number",
  fixed64: "number",
  sfixed32: "number",
  sfixed64: "number",
  float: "number",
  double: "number",
  string: "string",
  bytes: "Uint8Array",
};

const WORKFLOW_SETTINGS_STRING_DEFAULTS: Record<string, string> = {
  defaultDataset: DEFAULT_PROJECT_CONFIG.defaultSchema,
  defaultAssertionDataset: DEFAULT_PROJECT_CONFIG.assertionSchema,
};

export function loadDataformProtos(protosDir: string): protobuf.Root {
  const resolvedProtosDir = path.resolve(protosDir);
  const root = new protobuf.Root();
  const defaultResolvePath = root.resolvePath;

  root.resolvePath = (origin: string, target: string) => {
    if (target.startsWith("google/protobuf/")) {
      return defaultResolvePath.call(root, origin, target);
    }
    if (path.isAbsolute(target)) {
      return target;
    }
    return path.join(resolvedProtosDir, path.basename(target));
  };

  const protoPaths = BUNDLED_PROTO_FILES.map((file) => path.join(resolvedProtosDir, file));
  root.loadSync(protoPaths, {
    keepCase: false,
    alternateCommentMode: true,
  });
  root.resolveAll();
  return root;
}

export function extractWorkflowSettingsFields(root: protobuf.Root): IWorkflowSettingsFieldDoc[] {
  const workflowSettings = root.lookupType("dataform.WorkflowSettings");
  const isObjectMessageField = (field: protobuf.Field): boolean =>
    !(field instanceof protobuf.MapField) &&
    field.resolvedType instanceof protobuf.Type &&
    !field.resolvedType.options?.map_entry;

  const scalarAndMapFields = workflowSettings.fieldsArray.filter(
    (field) => !isObjectMessageField(field),
  );
  const objectFields = workflowSettings.fieldsArray.filter((field) => isObjectMessageField(field));

  return [...scalarAndMapFields, ...objectFields].map((field) => ({
    name: field.name,
    typeMarkdown: formatProtoFieldType(field),
    defaultMarkdown: formatWorkflowSettingsDefault(field),
    descriptionMarkdown: formatWorkflowSettingsDescription(field),
  }));
}

export function extractBundledProtoSchemas(
  root: protobuf.Root,
  protosDir: string,
): IBundledProtoFileDoc[] {
  const resolvedProtosDir = path.resolve(protosDir);
  const dataformNamespace = root.lookup("dataform") as protobuf.NamespaceBase;

  return BUNDLED_PROTO_FILES.map((fileName) => {
    const expectedPath = path.join(resolvedProtosDir, fileName);
    const inFile = dataformNamespace.nestedArray.filter(
      (nested) => nested.filename && path.resolve(nested.filename) === expectedPath,
    );

    if (fileName === "core.proto") {
      return { fileName, itemsMarkdown: summarizeCoreProto(root, inFile) };
    }
    if (fileName === "configs.proto") {
      return { fileName, itemsMarkdown: summarizeConfigsProto(root, inFile) };
    }
    if (fileName === "property_graph_config.proto") {
      return { fileName, itemsMarkdown: summarizePropertyGraphProto(root) };
    }
    if (fileName === "jit.proto") {
      return { fileName, itemsMarkdown: summarizeJitProto(inFile) };
    }
    if (fileName === "extension.proto") {
      return { fileName, itemsMarkdown: summarizeExtensionProto(inFile) };
    }
    return {
      fileName,
      itemsMarkdown: inFile
        .filter((nested): nested is protobuf.Type => nested instanceof protobuf.Type)
        .map((msg) => `\`${msg.name}\``),
    };
  });
}

function summarizeCoreProto(root: protobuf.Root, inFile: protobuf.ReflectionObject[]): string[] {
  const compiledGraph = root.lookupType("dataform.CompiledGraph");
  const referencedByGraph = new Set<string>();
  let hasTarget = false;
  for (const field of compiledGraph.fieldsArray) {
    if (
      !(field instanceof protobuf.MapField) &&
      field.resolvedType instanceof protobuf.Type &&
      field.resolvedType.fullName.startsWith(".dataform.") &&
      field.resolvedType.name !== "GraphErrors"
    ) {
      referencedByGraph.add(field.resolvedType.name);
      for (const childField of field.resolvedType.fieldsArray) {
        if (childField.resolvedType?.name === "Target") {
          hasTarget = true;
        }
      }
    }
  }

  const orderedNames: string[] = ["CompiledGraph"];
  for (const name of referencedByGraph) {
    if (name !== "ProjectConfig" && !orderedNames.includes(name)) {
      orderedNames.push(name);
    }
  }
  if (hasTarget && !orderedNames.includes("Target")) {
    orderedNames.push("Target");
  }
  if (referencedByGraph.has("ProjectConfig")) {
    orderedNames.push("ProjectConfig");
  }
  for (const nested of inFile) {
    if (
      nested instanceof protobuf.Type &&
      nested.name.startsWith("CoreExecution") &&
      !orderedNames.includes(nested.name)
    ) {
      orderedNames.push(nested.name);
    }
  }
  return orderedNames.map((name) => `\`${name}\``);
}

function summarizeConfigsProto(root: protobuf.Root, inFile: protobuf.ReflectionObject[]): string[] {
  const actionConfig = root.lookupType("dataform.ActionConfig");
  const actionOneof = actionConfig.oneofs?.action;
  const actionVariants = (actionOneof ? actionOneof.fieldsArray : [])
    .filter(
      (field) =>
        field.resolvedType instanceof protobuf.Type &&
        field.resolvedType.filename === actionConfig.filename,
    )
    .map((field) => `\`${field.resolvedType!.name}\``);

  const items: string[] = [];
  for (const nested of inFile) {
    if (!(nested instanceof protobuf.Type)) {
      continue;
    }
    if (nested.name === "ActionConfig" && actionVariants.length > 0) {
      items.push(`\`ActionConfig\` (${actionVariants.join(", ")})`);
    } else if (nested.name === "WorkflowSettings" || nested.name === "ActionConfigs") {
      items.push(`\`${nested.name}\``);
    }
  }
  return items;
}

function summarizePropertyGraphProto(root: protobuf.Root): string[] {
  const graphConfig = root.lookupType("dataform.PropertyGraphConfig");
  const nestedTypes: string[] = [];
  for (const field of graphConfig.fieldsArray) {
    if (!(field.resolvedType instanceof protobuf.Type)) {
      continue;
    }
    nestedTypes.push(field.resolvedType.name);
    if (field.resolvedType.name === "SemanticModelConfig") {
      for (const modelField of field.resolvedType.fieldsArray) {
        if (
          modelField.resolvedType instanceof protobuf.Type &&
          !nestedTypes.includes(modelField.resolvedType.name)
        ) {
          nestedTypes.push(modelField.resolvedType.name);
        }
      }
    }
  }
  return [
    nestedTypes.length > 0
      ? `\`${graphConfig.name}\` (${nestedTypes.map((name) => `\`${name}\``).join(", ")})`
      : `\`${graphConfig.name}\``,
  ];
}

function summarizeJitProto(inFile: protobuf.ReflectionObject[]): string[] {
  const services = inFile
    .filter((nested): nested is protobuf.Service => nested instanceof protobuf.Service)
    .map((svc) => `\`${svc.name}\` service`);
  const jitMessages = inFile
    .filter(
      (nested): nested is protobuf.Type =>
        nested instanceof protobuf.Type && nested.name.startsWith("Jit"),
    )
    .map((msg) => `\`${msg.name}\``);
  return [...services, ...jitMessages];
}

function summarizeExtensionProto(inFile: protobuf.ReflectionObject[]): string[] {
  const messages = inFile.filter(
    (nested): nested is protobuf.Type => nested instanceof protobuf.Type,
  );
  const enums = inFile
    .filter((nested): nested is protobuf.Enum => nested instanceof protobuf.Enum)
    .map((enm) => `\`${enm.name}\``);
  return messages.map((msg) =>
    enums.length > 0 ? `\`${msg.name}\` (${enums.join(", ")})` : `\`${msg.name}\``,
  );
}

function formatProtoFieldType(field: protobuf.Field): string {
  if (field instanceof protobuf.MapField) {
    const keyType = SCALAR_TYPE_LABELS[field.keyType] || field.keyType;
    const valueType = SCALAR_TYPE_LABELS[field.type] || field.type;
    return `\`map<${keyType}, ${valueType}>\``;
  }
  if (field.resolvedType instanceof protobuf.Type) {
    return field.repeated ? "`object[]`" : "`object`";
  }
  const baseType = field.resolvedType
    ? field.resolvedType.name
    : SCALAR_TYPE_LABELS[field.type] || field.type;
  return field.repeated ? `\`${baseType}[]\`` : `\`${baseType}\``;
}

function formatWorkflowSettingsDefault(field: protobuf.Field): string {
  const defaultString = WORKFLOW_SETTINGS_STRING_DEFAULTS[field.name];
  if (defaultString !== undefined) {
    return `\`${JSON.stringify(defaultString)}\``;
  }
  if (field instanceof protobuf.MapField) {
    return "`{}`";
  }
  if (field.repeated) {
    return "`[]`";
  }
  if (field.type === "bool") {
    return "`false`";
  }
  return "—";
}

function formatWorkflowSettingsDescription(field: protobuf.Field): string {
  const raw = field.comment || "";
  let summary = extractFirstProtoSentence(raw);

  // Preserve an explicit Markdown link sentence (`See [Guide](https://...).`) when present.
  const seeLinkMatch = /\b(See\s+\[[^\]]+\]\(https:\/\/[^)]+\)\.)/.exec(
    raw.replace(/\s+/g, " ").trim(),
  );
  if (
    field.resolvedType instanceof protobuf.Type &&
    !field.resolvedType.options?.map_entry &&
    !summary.includes("{")
  ) {
    const subfields = field.resolvedType.fieldsArray.map((sub) => `\`${sub.name}\``);
    if (subfields.length > 0 && summary.endsWith(".")) {
      summary = `${summary.slice(0, -1)} (${subfields.join(", ")}).`;
    }
  }
  if (seeLinkMatch && !summary.includes(seeLinkMatch[1])) {
    summary = `${summary} ${seeLinkMatch[1]}`;
  }
  return summary;
}

export function extractFirstProtoSentence(rawComment: string): string {
  const normalized = rawComment
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(?:Optional|Required)\.\s+/i, "");
  if (!normalized) {
    return "";
  }
  // Match up to the first sentence-ending period, ignoring abbreviations like "e.g." and "i.e.".
  const match = /^(.+?(?<!\be\.g)(?<!\bi\.e)\.)(?:\s|$)/i.exec(normalized);
  return match ? match[1] : normalized;
}
