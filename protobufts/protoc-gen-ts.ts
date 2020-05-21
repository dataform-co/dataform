import { google } from "df/protobufts/ts-protoc-protos";
import * as fs from "fs";

function generateFiles(
  request: google.protobuf.compiler.ICodeGeneratorRequest
): google.protobuf.compiler.ICodeGeneratorResponse {
  const fileTypeMapping = getTypeFileMapping(request.protoFile);

  const fileDescriptorMapping = new Map<string, google.protobuf.IFileDescriptorProto>();
  request.protoFile.forEach(fileDescriptorProto =>
    fileDescriptorMapping.set(fileDescriptorProto.name, fileDescriptorProto)
  );

  const parameters = parseGeneratorParameters(request.parameter);

  return {
    file: request.fileToGenerate.map(file => ({
      name: getGeneratedTypescriptFilename(file),
      content: getFileContent(fileDescriptorMapping.get(file), fileTypeMapping, parameters)
    }))
  };
}

interface ITypeLocation {
  importProtoFile: string;
  importName: string;
  typescriptTypeName: string;
}

function getTypeFileMapping(fileDescriptorProtos: google.protobuf.IFileDescriptorProto[]) {
  const typeFileMapping = new Map<string, ITypeLocation>();

  const insertMappings = (
    filename: string,
    packageParts: string[],
    nestedTypeParts: string[],
    descriptorProtos: google.protobuf.IDescriptorProto[],
    enumDescriptorProtos: google.protobuf.IEnumDescriptorProto[]
  ) => {
    const importName = getImportName(filename);
    descriptorProtos.forEach(descriptorProto => {
      const typescriptTypeName = nestedTypeParts.concat(descriptorProto.name).join(".");
      typeFileMapping.set(packageParts.concat(typescriptTypeName).join("."), {
        importProtoFile: filename,
        importName,
        typescriptTypeName
      });
      insertMappings(
        filename,
        packageParts,
        nestedTypeParts.concat(descriptorProto.name),
        descriptorProto.nestedType,
        descriptorProto.enumType
      );
    });
    enumDescriptorProtos.forEach(enumDescriptorProto => {
      const typescriptTypeName = nestedTypeParts.concat(enumDescriptorProto.name).join(".");
      typeFileMapping.set(packageParts.concat(typescriptTypeName).join("."), {
        importProtoFile: filename,
        importName,
        typescriptTypeName
      });
    });
  };

  fileDescriptorProtos.forEach(fileDescriptorProto =>
    insertMappings(
      fileDescriptorProto.name,
      fileDescriptorProto.package.split("."),
      [],
      fileDescriptorProto.messageType,
      fileDescriptorProto.enumType
    )
  );

  return typeFileMapping;
}

interface IGeneratorParameters {
  importPrefix?: string;
}

function parseGeneratorParameters(parameters: string) {
  const parsed: IGeneratorParameters = {};
  parameters.split(",").forEach(parameter => {
    const [key, value] = parameter.split("=");
    if (key === "import_prefix") {
      parsed.importPrefix = value;
    }
  });
  return parsed;
}

function getFileContent(
  fileDescriptorProto: google.protobuf.IFileDescriptorProto,
  fileTypeMapping: Map<string, ITypeLocation>,
  parameters: IGeneratorParameters
) {
  const needsLongImport = (descriptorProto: google.protobuf.IDescriptorProto) => {
    if (
      descriptorProto.field.some(
        fieldDescriptorProto =>
          type(
            fieldDescriptorProto.type,
            fieldDescriptorProto.typeName,
            fileTypeMapping,
            fileDescriptorProto.name
          ) === "Long"
      )
    ) {
      return true;
    }
    if (
      descriptorProto.nestedType.some(nestedDescriptorProto =>
        needsLongImport(nestedDescriptorProto)
      )
    ) {
      return true;
    }
    return false;
  };
  return `// AUTOMATICALLY GENERATED CODE. DO NOT EDIT.

// IMPORTS
${getImportLines(
  fileDescriptorProto.dependency,
  fileDescriptorProto.messageType.some(needsLongImport),
  parameters.importPrefix
).join("\n")}

// MESSAGES
${fileDescriptorProto.messageType
  .map(descriptorProto => getMessage(descriptorProto, fileTypeMapping, fileDescriptorProto.name))
  .join("\n\n")}

// ENUMS
${fileDescriptorProto.enumType
  .map(enumDescriptorProto => getEnum(enumDescriptorProto))
  .join("\n\n")}
`;
}

function getImportLines(
  protoDependencies: string[],
  needsLongImport: boolean,
  importPrefix?: string
) {
  const imports = protoDependencies.map(dependency => {
    let importPath = getGeneratedTypescriptFilename(dependency);
    if (importPrefix) {
      importPath = `${importPrefix}/${importPath}`;
    }
    return `import * as ${getImportName(dependency)} from "${importPath}"`;
  });
  if (needsLongImport) {
    imports.push('import Long from "long";');
  }
  return imports;
}

function getImportName(dependency: string) {
  return dependency
    .split("/")
    .slice(-1)[0]
    .split(".")
    .slice(0, -1)[0];
}

function getMessage(
  descriptorProto: google.protobuf.IDescriptorProto,
  fileTypeMapping: Map<string, ITypeLocation>,
  currentProtoFile: string,
  indentCount: number = 0
): string {
  const message = `export class ${descriptorProto.name} {
${descriptorProto.field
  .map(
    fieldDescriptorProto =>
      `  public ${fieldDescriptorProto.jsonName}: ${type(
        fieldDescriptorProto.type,
        fieldDescriptorProto.typeName,
        fileTypeMapping,
        currentProtoFile
      )} = ${defaultValue(fieldDescriptorProto.type, fieldDescriptorProto.typeName)};`
  )
  .join("\n")}
}`;
  if (descriptorProto.nestedType.length === 0 && descriptorProto.enumType.length === 0) {
    return indent(message, indentCount);
  }
  return indent(
    `${message}

export namespace ${descriptorProto.name} {
  // MESSAGES
${descriptorProto.nestedType
  .map(nestedDescriptorProto =>
    getMessage(nestedDescriptorProto, fileTypeMapping, currentProtoFile, indentCount + 1)
  )
  .join("\n\n")}

  // ENUMS
${descriptorProto.enumType
  .map(nestedEnumDescriptorProto => getEnum(nestedEnumDescriptorProto, indentCount + 1))
  .join("\n\n")}
}`,
    indentCount
  );
}

function type(
  typeValue: google.protobuf.FieldDescriptorProto.Type,
  typeName: string,
  fileTypeMapping: Map<string, ITypeLocation>,
  currentProtoFile: string
) {
  switch (typeValue) {
    case google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE: // TODO: is this right?
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT: // TODO: is this right?
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT32:
      return "number";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64:
      return "Long";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BOOL:
      return "boolean";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_STRING:
      return "string";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
      throw new Error("GROUP is unsupported.");
    case google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE:
      const typeLocation = fileTypeMapping.get(typeName.slice(1));
      return typeLocation.importProtoFile === currentProtoFile
        ? typeLocation.typescriptTypeName
        : `${typeLocation.importName}.${typeLocation.typescriptTypeName}`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
      return "Uint8Array";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM:
      // TODO
      return "number /* " + typeName + " */";
    default:
      throw new Error(`Unrecognized field type: ${typeValue}`);
  }
}

function defaultValue(typeValue: google.protobuf.FieldDescriptorProto.Type, typeName: string) {
  switch (typeValue) {
    case google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE: // TODO: is this right?
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT: // TODO: is this right?
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT32:
      return "0";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64:
      return "Long.fromNumber(0)";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BOOL:
      return false;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_STRING:
      return '""';
    case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
      throw new Error("GROUP is unsupported.");
    case google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE:
      return "null";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
      return "new Uint8Array()";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM:
      // TODO
      return "0";
    default:
      throw new Error(`Unrecognized field type: ${typeValue}`);
  }
}

function getEnum(
  enumDescriptorProto: google.protobuf.IEnumDescriptorProto,
  indentCount: number = 0
) {
  return indent(
    `export enum ${enumDescriptorProto.name} {
}`,
    indentCount
  );
}

function getGeneratedTypescriptFilename(protoFilename: string) {
  return protoFilename.replace(".proto", ".ts");
}

function indent(lines: string, indentCount: number) {
  return lines
    .split("\n")
    .map(line => `${"  ".repeat(indentCount)}${line}`.trimRight())
    .join("\n");
}

process.stdout.write(
  Buffer.from(
    google.protobuf.compiler.CodeGeneratorResponse.encode(
      generateFiles(
        google.protobuf.compiler.CodeGeneratorRequest.decode(fs.readFileSync("/dev/stdin"))
      )
    ).finish()
  )
);
