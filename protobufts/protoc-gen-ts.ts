import * as fs from "fs";

import { google } from "df/protobufts/ts-protoc-protos";

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
          type(fieldDescriptorProto, fileTypeMapping, fileDescriptorProto.name) === "Long"
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
  return `// Code generated automatically by protoc-gen-ts.

${getImportLines(
  fileDescriptorProto.dependency,
  fileDescriptorProto.messageType.some(needsLongImport),
  parameters.importPrefix
).join("\n")}

${fileDescriptorProto.messageType
  .map(descriptorProto => getMessage(descriptorProto, fileTypeMapping, fileDescriptorProto.name))
  .join("\n\n")}

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
  imports.push('import { Serializer } from "df/protobufts/runtime/serialize";');
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
  currentProtoFile: string
): string {
  // TODO: better names for oneof fields.
  const message = `export class ${descriptorProto.name} {
  public static create(params: {
${descriptorProto.field
  .filter(fieldDescriptorProto => !fieldDescriptorProto.hasOwnProperty("oneofIndex"))
  .map(
    fieldDescriptorProto =>
      `    ${fieldDescriptorProto.jsonName}?: ${type(
        fieldDescriptorProto,
        fileTypeMapping,
        currentProtoFile
      )};`
  )
  .join("\n")}
${descriptorProto.oneofDecl.map(
  oneofDescriptorProto => `    ${oneofDescriptorProto.name}?: ${oneofType()};`
)}
  }) {
    const newProto = new ${descriptorProto.name}();
${descriptorProto.field
  .filter(fieldDescriptorProto => !fieldDescriptorProto.hasOwnProperty("oneofIndex"))
  .map(
    fieldDescriptorProto =>
      `    if ('${fieldDescriptorProto.jsonName}' in params) { newProto.${fieldDescriptorProto.jsonName} = params.${fieldDescriptorProto.jsonName}; }`
  )
  .join("\n")}
${descriptorProto.oneofDecl.map(
  oneofDescriptorProto =>
    `    if ('${oneofDescriptorProto.name}' in params) { newProto.${oneofDescriptorProto.name} = params.${oneofDescriptorProto.name}; }`
)}
    return newProto;
  }
${descriptorProto.field
  .filter(fieldDescriptorProto => !fieldDescriptorProto.hasOwnProperty("oneofIndex"))
  .map(
    fieldDescriptorProto =>
      `  public ${fieldDescriptorProto.jsonName}${
        hasDefaultValue(fieldDescriptorProto) ? "" : "?"
      }: ${type(fieldDescriptorProto, fileTypeMapping, currentProtoFile)}${
        hasDefaultValue(fieldDescriptorProto) ? ` = ${defaultValue(fieldDescriptorProto)}` : ""
      };`
  )
  .join("\n")}
${descriptorProto.oneofDecl.map(
  oneofDescriptorProto => `  public ${oneofDescriptorProto.name}?: ${oneofType()};`
)}

  public serialize(): Uint8Array {
    const serializer = new Serializer();
${descriptorProto.field
  // TODO: serializer code for oneofs.
  .filter(fieldDescriptorProto => !fieldDescriptorProto.hasOwnProperty("oneofIndex"))
  .map(fieldDescriptorProto => {
    const serializerCall = `serializer.${serializerMethodName(fieldDescriptorProto)}(${
      fieldDescriptorProto.number
    }, ${isPacked(fieldDescriptorProto)}, this.${fieldDescriptorProto.jsonName})`;
    return `    if (${shouldSerialize(fieldDescriptorProto)}) {
      ${serializerCall};
    }`;
  })
  .join("\n")}
    return serializer.finish();
  }
}`;
  if (descriptorProto.nestedType.length === 0 && descriptorProto.enumType.length === 0) {
    return message;
  }
  const nestedMessages = descriptorProto.nestedType
    .map(nestedDescriptorProto =>
      indent(getMessage(nestedDescriptorProto, fileTypeMapping, currentProtoFile))
    )
    .join("\n\n");
  const nestedEnums = descriptorProto.enumType
    .map(nestedEnumDescriptorProto => indent(getEnum(nestedEnumDescriptorProto)))
    .join("\n\n");
  return `${message}

export namespace ${descriptorProto.name} {${
    descriptorProto.nestedType.length > 0 ? `\n${nestedMessages}\n` : ""
  }${descriptorProto.enumType.length > 0 ? `\n${nestedEnums}\n` : ""}}`;
}

function oneofType() {
  return `{ kind: "oneofInt32Field", value: number } | { kind: "oneofStringField", value: string }`;
}

function type(
  fieldDescriptorProto: google.protobuf.IFieldDescriptorProto,
  fileTypeMapping: Map<string, ITypeLocation>,
  currentProtoFile: string
) {
  const baseType = () => {
    switch (fieldDescriptorProto.type) {
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
      case google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM:
        const typeLocation = fileTypeMapping.get(fieldDescriptorProto.typeName.slice(1));
        return typeLocation.importProtoFile === currentProtoFile
          ? typeLocation.typescriptTypeName
          : `${typeLocation.importName}.${typeLocation.typescriptTypeName}`;
      case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
        return "Uint8Array";
      default:
        throw new Error(`Unrecognized field type: ${fieldDescriptorProto.type}`);
    }
  };
  if (fieldDescriptorProto.label !== google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
    return baseType();
  }
  return `${baseType()}[]`;
}

function hasDefaultValue(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
  if (fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
    return true;
  }
  return fieldDescriptorProto.type !== google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE;
}

function defaultValue(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
  if (fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
    return "[]";
  }
  switch (fieldDescriptorProto.type) {
    case google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM:
      return "0";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64:
      return "Long.ZERO";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64:
      return "Long.UZERO";
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BOOL:
      return false;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_STRING:
      return '""';
    case google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE:
      throw new Error("Message fields have no default value.");
    case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
      throw new Error("GROUP is unsupported.");
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
      return "new Uint8Array()";
    default:
      throw new Error(`Unrecognized field type: ${fieldDescriptorProto.type}`);
  }
}

function shouldSerialize(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
  if (fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
    return `this.${fieldDescriptorProto.jsonName}.length > 0`;
  }
  switch (fieldDescriptorProto.type) {
    case google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT32:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM:
      return `this.${fieldDescriptorProto.jsonName} !== 0`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_INT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64:
    case google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64:
      return `!this.${fieldDescriptorProto.jsonName}.isZero()`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BOOL:
      return `this.${fieldDescriptorProto.jsonName}`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_STRING:
      return `this.${fieldDescriptorProto.jsonName} !== ""`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE:
      return `!!this.${fieldDescriptorProto.jsonName}`;
    case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
      throw new Error("GROUP is unsupported.");
    case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
      return `this.${fieldDescriptorProto.jsonName}.length > 0`;
    default:
      throw new Error(`Unrecognized field type: ${fieldDescriptorProto.type}`);
  }
}

function serializerMethodName(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
  const typeString = google.protobuf.FieldDescriptorProto.Type[fieldDescriptorProto.type];
  return typeString.replace("TYPE_", "").toLowerCase();
}

function isPacked(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
  if (fieldDescriptorProto.label !== google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
    return false;
  }
  if (!fieldDescriptorProto.options) {
    return true;
  }
  if (!("packed" in fieldDescriptorProto.options)) {
    return true;
  }
  return fieldDescriptorProto.options.packed;
}

function getEnum(enumDescriptorProto: google.protobuf.IEnumDescriptorProto) {
  return `export enum ${enumDescriptorProto.name} {
${enumDescriptorProto.value
  .map(
    enumValueDescriptorProto =>
      `  ${enumValueDescriptorProto.name} = ${enumValueDescriptorProto.number},`
  )
  .join("\n")}
}`;
}

function getGeneratedTypescriptFilename(protoFilename: string) {
  return protoFilename.replace(".proto", ".ts");
}

function indent(lines: string) {
  return lines
    .split("\n")
    .map(line => `  ${line}`.trimRight())
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
