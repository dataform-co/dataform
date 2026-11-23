import * as path from "path";
import * as protobuf from "protobufjs";

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

export function loadDataformProtos(
  protosDir: string,
  entryFiles: readonly string[] = BUNDLED_PROTO_FILES,
): protobuf.Root {
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

  const protoPaths = entryFiles.map((file) => path.join(resolvedProtosDir, file));
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
  const paragraphs = splitProtoParagraphs(field.comment || "");
  if (paragraphs.length === 0) {
    return "";
  }
  let summary = stripLeadingOptionalPrefix(paragraphs[0]);
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
  // If a second paragraph starts with `See [...]`, append that cross-reference sentence after any
  // subfield list.
  const seeParagraph = paragraphs.slice(1).find((p) => p.startsWith("See ["));
  if (seeParagraph) {
    summary = `${summary} ${seeParagraph}`;
  }
  return summary;
}

export function extractFirstProtoParagraph(rawComment: string): string {
  const paragraphs = splitProtoParagraphs(rawComment);
  return paragraphs.length > 0 ? stripLeadingOptionalPrefix(paragraphs[0]) : "";
}

function splitProtoParagraphs(rawComment: string): string[] {
  return rawComment
    .trim()
    .split(/\r?\n\s*\r?\n/)
    .map((paragraph) => paragraph.replace(/\s+/g, " ").trim())
    .filter((paragraph) => paragraph.length > 0);
}

function stripLeadingOptionalPrefix(paragraph: string): string {
  return paragraph.replace(/^Optional\.\s+/i, "");
}
