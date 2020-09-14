import { google } from "df/protobufts/ts-protoc-protos";

export interface ITypeMetadata<T extends IEnumDescriptor | IMessageDescriptor> {
  file: {
    protobuf: {
      name: string;
    };
    typescript: {
      name: string;
      importAs: string;
    };
  };
  protobufType: {
    fullyQualifiedName: string;
  } & T;
  typescriptType: {
    parentMessages: string[];
    name: string;
  };
}

export interface IMessageDescriptor {
  isEnum: false;
  descriptorProto: google.protobuf.IDescriptorProto;
  fields: google.protobuf.IFieldDescriptorProto[];
  oneofs: Array<{
    name: string;
    fields: google.protobuf.IFieldDescriptorProto[];
  }>;
}

export interface IEnumDescriptor {
  isEnum: true;
  enumDescriptorProto: google.protobuf.IEnumDescriptorProto;
}

export class TypeRegistry {
  public static fromFiles(fileDescriptorProtos: google.protobuf.IFileDescriptorProto[]) {
    const types: Array<ITypeMetadata<IEnumDescriptor | IMessageDescriptor>> = [];
    fileDescriptorProtos.forEach(fileDescriptorProto => {
      const file = {
        protobuf: {
          name: fileDescriptorProto.name
        },
        typescript: {
          name: TypeRegistry.protobufFilenameToTypescriptFilename(fileDescriptorProto.name),
          importAs: TypeRegistry.protobufFilenameToImportAlias(fileDescriptorProto.name)
        }
      };

      const appendEnumMetadatas = (
        enumDescriptorProtos: google.protobuf.IEnumDescriptorProto[],
        parentMessages: string[]
      ) => {
        enumDescriptorProtos.forEach(enumDescriptorProto => {
          types.push({
            file,
            protobufType: {
              fullyQualifiedName: [
                fileDescriptorProto.package,
                ...parentMessages,
                enumDescriptorProto.name
              ].join("."),
              isEnum: true,
              enumDescriptorProto
            },
            typescriptType: {
              parentMessages,
              name: enumDescriptorProto.name
            }
          });
        });
      };

      const appendMessageMetadatas = (
        descriptorProtos: google.protobuf.IDescriptorProto[],
        parentMessages: string[]
      ) => {
        descriptorProtos.forEach(descriptorProto => {
          types.push({
            file,
            protobufType: {
              fullyQualifiedName: [
                fileDescriptorProto.package,
                ...parentMessages,
                descriptorProto.name
              ].join("."),
              isEnum: false,
              descriptorProto,
              fields: descriptorProto.field.filter(
                fieldDescriptorProto => !fieldDescriptorProto.hasOwnProperty("oneofIndex")
              ),
              oneofs: descriptorProto.oneofDecl.map((oneofDeclaration, index) => ({
                name: oneofDeclaration.name,
                fields: descriptorProto.field.filter(
                  fieldDescriptorProto =>
                    fieldDescriptorProto.hasOwnProperty("oneofIndex") &&
                    fieldDescriptorProto.oneofIndex === index
                )
              }))
            },
            typescriptType: {
              parentMessages,
              name: descriptorProto.name
            }
          });
          appendMessageMetadatas(descriptorProto.nestedType, [
            ...parentMessages,
            descriptorProto.name
          ]);
          appendEnumMetadatas(descriptorProto.enumType, [...parentMessages, descriptorProto.name]);
        });
      };

      appendMessageMetadatas(fileDescriptorProto.messageType, []);
      appendEnumMetadatas(fileDescriptorProto.enumType, []);
    });

    return new TypeRegistry(types);
  }

  public static protobufFilenameToTypescriptFilename(filename: string) {
    return filename.replace(".proto", ".ts");
  }

  public static protobufFilenameToImportAlias(filename: string) {
    return filename
      .split("/")
      .slice(-1)[0]
      .split(".")
      .slice(0, -1)[0];
  }

  constructor(
    private readonly allTypes: Array<ITypeMetadata<IEnumDescriptor | IMessageDescriptor>>
  ) {}

  public forTypescriptFilename(filename: string, parentMessages: string[] = []) {
    return this.allTypes.filter(
      type =>
        type.file.typescript.name === filename &&
        type.typescriptType.parentMessages.join(".") === parentMessages.join(".")
    );
  }

  public typescriptTypeFromProtobufType(
    type: google.protobuf.FieldDescriptorProto.Type,
    typeName: string,
    insideMessage: ITypeMetadata<IMessageDescriptor>
  ): string {
    switch (type) {
      case google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE:
      case google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT:
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
        const typeMetadata = this.allTypes.find(
          typeMetadata => typeMetadata.protobufType.fullyQualifiedName === typeName.slice(1)
        );
        if (
          typeMetadata.protobufType.isEnum === false &&
          typeMetadata.protobufType.descriptorProto.options?.mapEntry
        ) {
          const keyField = typeMetadata.protobufType.descriptorProto.field[0];
          const keyType = this.typescriptTypeFromProtobufType(
            keyField.type,
            keyField.typeName,
            insideMessage
          );
          const valueField = typeMetadata.protobufType.descriptorProto.field[1];
          const valueType = this.typescriptTypeFromProtobufType(
            valueField.type,
            valueField.typeName,
            insideMessage
          );
          return `Map<${keyType}, ${valueType}>`;
        }
        const typescriptTypeName = [
          ...typeMetadata.typescriptType.parentMessages,
          typeMetadata.typescriptType.name
        ].join(".");
        if (typeMetadata.file.typescript.name === insideMessage.file.typescript.name) {
          return typescriptTypeName;
        }
        return `${typeMetadata.file.typescript.importAs}.${typescriptTypeName}`;
      case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
        return "Uint8Array";
      default:
        throw new Error(`Unrecognized field type: ${type}`);
    }
  }

  public typescriptTypeFromOneofType(
    fields: Array<{
      fieldName: string;
      type: google.protobuf.FieldDescriptorProto.Type;
      typeName: string;
    }>,
    insideMessage: ITypeMetadata<IMessageDescriptor>
  ) {
    return fields
      .map(
        ({ fieldName, type, typeName }) =>
          `{ field: "${fieldName}", value: ${this.typescriptTypeFromProtobufType(
            type,
            typeName,
            insideMessage
          )} }`
      )
      .join(" | ");
  }

  public forFieldDescriptor(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (
      ![
        google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM,
        google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE
      ].includes(fieldDescriptorProto.type)
    ) {
      throw new Error(`Field ${fieldDescriptorProto.name} is not an enum or message type.`);
    }
    return this.allTypes.find(
      typeMetadata =>
        typeMetadata.protobufType.fullyQualifiedName === fieldDescriptorProto.typeName.slice(1)
    );
  }
}
