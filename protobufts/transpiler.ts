import { IGeneratorParameters } from "df/protobufts/parameters";
import { google } from "df/protobufts/ts-protoc-protos";
import {
  IEnumDescriptor,
  IMessageDescriptor,
  ITypeMetadata,
  TypeRegistry
} from "df/protobufts/types";

const IMPORT_LONG = 'import Long from "long";';
const IMPORT_JSON_SUPPORT = 'import { toJsonValue } from "df/protobufts/runtime/json_support";';
const IMPORT_SERIALIZATION =
  'import { Decoders, Deserializer, Serializer } from "df/protobufts/runtime/serialize";';

const LONG_TYPES = [
  google.protobuf.FieldDescriptorProto.Type.TYPE_INT64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64
];

export class FileTranspiler {
  public static forProtobufFile(
    file: google.protobuf.IFileDescriptorProto,
    types: TypeRegistry,
    parameters: IGeneratorParameters
  ) {
    const outputFilename = TypeRegistry.protobufFilenameToTypescriptFilename(file.name);
    return new FileTranspiler(
      file,
      outputFilename,
      types.forTypescriptFilename(outputFilename),
      types,
      parameters
    );
  }

  constructor(
    private readonly file: google.protobuf.IFileDescriptorProto,
    private readonly outputFilename: string,
    private readonly typesToGenerate: Array<ITypeMetadata<IEnumDescriptor | IMessageDescriptor>>,
    private readonly allTypes: TypeRegistry,
    private readonly parameters: IGeneratorParameters
  ) {}

  public generateFileContent(): google.protobuf.compiler.CodeGeneratorResponse.IFile {
    return {
      name: this.outputFilename,
      content: `${this.getImportLines().join("\n")}

${[
  ...this.typesToGenerate
    .filter((type): type is ITypeMetadata<IMessageDescriptor> => type.protobufType.isEnum === false)
    .map(type => MessageTranspiler.forMessage(type, this.allTypes).generateMessage()),
  ...this.typesToGenerate
    .filter((type): type is ITypeMetadata<IEnumDescriptor> => type.protobufType.isEnum === true)
    .map(type => EnumTranspiler.forEnum(type).generateEnum())
].join("\n\n")}`
    };
  }

  private getImportLines() {
    return [
      ...this.file.dependency.map(dependencyProtobufFilename => {
        const typescriptFilename = TypeRegistry.protobufFilenameToTypescriptFilename(
          dependencyProtobufFilename
        );
        return `import * as ${TypeRegistry.protobufFilenameToImportAlias(
          dependencyProtobufFilename
        )} from "${
          this.parameters.importPrefix
            ? `${this.parameters.importPrefix}/${typescriptFilename}`
            : typescriptFilename
        }"`;
      }),
      ...(this.needsLongImport() ? [IMPORT_LONG] : []),
      IMPORT_JSON_SUPPORT,
      IMPORT_SERIALIZATION
    ];
  }

  private needsLongImport() {
    return this.typesToGenerate.some(
      type =>
        type.protobufType.isEnum === false &&
        type.protobufType.descriptorProto.field.some(fieldDescriptorProto =>
          LONG_TYPES.includes(fieldDescriptorProto.type)
        )
    );
  }
}

const PACKABLE_TYPES = [
  google.protobuf.FieldDescriptorProto.Type.TYPE_DOUBLE,
  google.protobuf.FieldDescriptorProto.Type.TYPE_FLOAT,
  google.protobuf.FieldDescriptorProto.Type.TYPE_INT64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_UINT64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_INT32,
  google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_FIXED32,
  google.protobuf.FieldDescriptorProto.Type.TYPE_BOOL,
  google.protobuf.FieldDescriptorProto.Type.TYPE_UINT32,
  google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED32,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SFIXED64,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SINT32,
  google.protobuf.FieldDescriptorProto.Type.TYPE_SINT64
];

class MessageTranspiler {
  public static forMessage(type: ITypeMetadata<IMessageDescriptor>, types: TypeRegistry) {
    const fieldTypeNames = new Map<string, string>();
    type.protobufType.fields.forEach(fieldDescriptorProto =>
      fieldTypeNames.set(
        fieldDescriptorProto.name,
        types.typescriptTypeFromProtobufType(
          fieldDescriptorProto.type,
          fieldDescriptorProto.typeName,
          type
        )
      )
    );
    type.protobufType.oneofs.forEach(oneof =>
      fieldTypeNames.set(
        oneof.name,
        types.typescriptTypeFromOneofType(
          oneof.fields.map(fieldDescriptorProto => ({
            fieldName: fieldDescriptorProto.jsonName,
            type: fieldDescriptorProto.type,
            typeName: fieldDescriptorProto.typeName
          })),
          type
        )
      )
    );
    return new MessageTranspiler(type, types, fieldTypeNames);
  }

  constructor(
    private readonly type: ITypeMetadata<IMessageDescriptor>,
    private readonly types: TypeRegistry,
    private readonly fieldTypeNames: Map<string, string>
  ) {}

  public generateMessage(): string {
    const message = `export class ${this.type.typescriptType.name} {
  public static create(params: {
${[
  ...this.type.protobufType.fields.map(
    fieldDescriptorProto =>
      `    ${fieldDescriptorProto.jsonName}?: ${this.maybeArrayType(fieldDescriptorProto)};`
  ),
  ...this.type.protobufType.oneofs.map(
    oneof => `    ${oneof.name}?: ${this.fieldTypeNames.get(oneof.name)}`
  )
].join("\n")}
  } = {}) {
    const newProto = new ${this.type.typescriptType.name}();
${[
  ...this.type.protobufType.fields.map(fieldDescriptorProto => fieldDescriptorProto.jsonName),
  ...this.type.protobufType.oneofs.map(oneof => oneof.name)
]
  .map(
    fieldName =>
      `    if ('${fieldName}' in params) { newProto.${fieldName} = params.${fieldName}; }`
  )
  .join("\n")}
    return newProto;
  }

  public static deserialize(bytes: Uint8Array) {
    const deserializer = new Deserializer(bytes);
    const newProto = new ${this.type.typescriptType.name}();
    for (const { fieldNumber, reader } of deserializer.deserialize()) {
      switch (fieldNumber) {
${indent(
  [
    ...this.type.protobufType.fields.map(fieldDescriptorProto =>
      Serialization.forField(fieldDescriptorProto, this.types, this.type)
    ),
    ...this.type.protobufType.oneofs.map(oneof =>
      Serialization.forOneof(oneof.name, oneof.fields, this.types, this.type)
    )
  ]
    .map(serialization => serialization.callDeserializer())
    .join("\n"),
  4
)}
      }
    }
    return newProto;
  }

  private static readonly decoders = {
${indent(
  [
    ...this.type.protobufType.fields.map(
      fieldDescriptorProto =>
        `${fieldDescriptorProto.number}: ${Decoders.for(
          fieldDescriptorProto,
          this.types,
          this.type
        ).instantiate()}`
    ),
    ...this.type.protobufType.oneofs
      .map(oneof =>
        oneof.fields.map(
          fieldDescriptorProto =>
            `${fieldDescriptorProto.number}: ${Decoders.for(
              fieldDescriptorProto,
              this.types,
              this.type,
              true
            ).instantiate()}`
        )
      )
      .flat()
  ].join(",\n"),
  2
)}
  };

${indent(
  [
    ...this.type.protobufType.fields.map(
      fieldDescriptorProto =>
        `public ${this.maybeOptionalMember(fieldDescriptorProto)}: ${this.maybeArrayType(
          fieldDescriptorProto
        )} = ${this.defaultValue(fieldDescriptorProto)};`
    ),
    ...this.type.protobufType.oneofs.map(
      oneof => `public ${oneof.name}?: ${this.fieldTypeNames.get(oneof.name)} = null;`
    )
  ].join("\n")
)}

  public serialize(): Uint8Array {
    const serializer = new Serializer();
${indent(
  [
    ...this.type.protobufType.fields.map(fieldDescriptorProto =>
      Serialization.forField(fieldDescriptorProto, this.types, this.type)
    ),
    ...this.type.protobufType.oneofs.map(oneof =>
      Serialization.forOneof(oneof.name, oneof.fields, this.types, this.type)
    )
  ]
    .map(serialization => serialization.callSerializer())
    .join("\n"),
  2
)}
    return serializer.finish();
  }

  public toJson() {
    const jsonObject: any = {};
${indent(
  [
    ...this.type.protobufType.fields.map(fieldDescriptorProto =>
      Serialization.forField(fieldDescriptorProto, this.types, this.type)
    ),
    ...this.type.protobufType.oneofs.map(oneof =>
      Serialization.forOneof(oneof.name, oneof.fields, this.types, this.type)
    )
  ]
    .map(serialization => serialization.setJsonObjectFields())
    .join("\n"),
  2
)}
    return jsonObject;
  }
}`;
    const nestedMessages = this.types
      .forTypescriptFilename(this.type.file.typescript.name, [
        ...this.type.typescriptType.parentMessages,
        this.type.typescriptType.name
      ])
      .filter(
        (type): type is ITypeMetadata<IMessageDescriptor> => type.protobufType.isEnum === false
      )
      .filter(type => !type.protobufType.descriptorProto.options?.mapEntry);
    const nestedEnums = this.types
      .forTypescriptFilename(this.type.file.typescript.name, [
        ...this.type.typescriptType.parentMessages,
        this.type.typescriptType.name
      ])
      .filter((type): type is ITypeMetadata<IEnumDescriptor> => type.protobufType.isEnum === true);
    if (nestedMessages.length === 0 && nestedEnums.length === 0) {
      return message;
    }
    return `${message}

export namespace ${this.type.typescriptType.name} {
${indent(
  [
    ...nestedMessages.map(type => MessageTranspiler.forMessage(type, this.types).generateMessage()),
    ...nestedEnums.map(type => EnumTranspiler.forEnum(type).generateEnum())
  ]
    .filter(str => !!str)
    .join("\n\n")
)}
}`;
  }

  private maybeArrayType(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    const fieldTypeName = this.fieldTypeNames.get(fieldDescriptorProto.name);
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return fieldTypeName;
      }
    }
    if (fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
      return `${fieldTypeName}[]`;
    }
    return fieldTypeName;
  }

  private maybeOptionalMember(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
      return fieldDescriptorProto.jsonName;
    }
    if (fieldDescriptorProto.type !== google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      return fieldDescriptorProto.jsonName;
    }
    return `${fieldDescriptorProto.jsonName}?`;
  }

  private defaultValue(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return `new ${this.fieldTypeNames.get(fieldDescriptorProto.name)}()`;
      }
    }
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
        return "null";
      case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
        throw new Error("GROUP is unsupported.");
      case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
        return "new Uint8Array()";
      default:
        throw new Error(`Unrecognized field type: ${fieldDescriptorProto.type}`);
    }
  }
}

class EnumTranspiler {
  public static forEnum(type: ITypeMetadata<IEnumDescriptor>) {
    return new EnumTranspiler(type);
  }

  constructor(private readonly type: ITypeMetadata<IEnumDescriptor>) {}

  public generateEnum() {
    return `export enum ${this.type.typescriptType.name} {
${this.type.protobufType.enumDescriptorProto.value
  .map(
    enumValueDescriptorProto =>
      `  ${enumValueDescriptorProto.name} = ${enumValueDescriptorProto.number},`
  )
  .join("\n")}
}`;
  }
}

class Serialization {
  public static forField(
    fieldDescriptorProto: google.protobuf.IFieldDescriptorProto,
    types: TypeRegistry,
    insideMessage: ITypeMetadata<IMessageDescriptor>
  ) {
    return new Serialization({ type: "normal", fieldDescriptorProto }, types, insideMessage);
  }

  public static forOneof(
    name: string,
    fieldDescriptorProtos: google.protobuf.IFieldDescriptorProto[],
    types: TypeRegistry,
    insideMessage: ITypeMetadata<IMessageDescriptor>
  ) {
    return new Serialization({ type: "oneof", name, fieldDescriptorProtos }, types, insideMessage);
  }

  private static isPacked(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (fieldDescriptorProto.label !== google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED) {
      return false;
    }
    if (!PACKABLE_TYPES.includes(fieldDescriptorProto.type)) {
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

  constructor(
    private readonly field:
      | { type: "normal"; fieldDescriptorProto: google.protobuf.IFieldDescriptorProto }
      | {
          type: "oneof";
          name: string;
          fieldDescriptorProtos: google.protobuf.IFieldDescriptorProto[];
        },
    private readonly types: TypeRegistry,
    private readonly insideMessage: ITypeMetadata<IMessageDescriptor>
  ) {}

  public callSerializer() {
    if (this.field.type === "oneof") {
      const name = this.field.name;
      return `if (!!this.${name}) {
  switch (this.${name}.field) {
${indent(
  this.field.fieldDescriptorProtos
    .map(
      fieldDescriptorProto =>
        `case "${fieldDescriptorProto.jsonName}": serializer.${this.serializerMethodName(
          fieldDescriptorProto
        )}(${fieldDescriptorProto.number}, false, this.${name}.value); break;`
    )
    .join("\n"),
  2
)}
  }
}`;
    }
    let isMap = false;
    let keyField: google.protobuf.IFieldDescriptorProto;
    let valueField: google.protobuf.IFieldDescriptorProto;
    if (
      this.field.fieldDescriptorProto.type ===
      google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE
    ) {
      const typeMetadata = this.types.forFieldDescriptor(this.field.fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${this.field.fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        isMap = true;
        keyField = typeMetadata.protobufType.descriptorProto.field[0];
        valueField = typeMetadata.protobufType.descriptorProto.field[1];
      }
    }
    return `if (${this.checkShouldSerialize(
      this.field.fieldDescriptorProto
    )}) { serializer.${this.serializerMethodName(this.field.fieldDescriptorProto)}(${
      this.field.fieldDescriptorProto.number
    }, ${Serialization.isPacked(this.field.fieldDescriptorProto)}, this.${
      this.field.fieldDescriptorProto.jsonName
    }${
      isMap
        ? `, (mapEntrySerializer, key) => { mapEntrySerializer.${this.serializerMethodName(
            keyField
          )}(1, false, key); }, (mapEntrySerializer, value) => { mapEntrySerializer.${this.serializerMethodName(
            valueField
          )}(1, false, value); }`
        : ""
    }); }`;
  }

  public callDeserializer() {
    if (this.field.type === "oneof") {
      const memberName = this.field.name;
      return this.field.fieldDescriptorProtos
        .map(
          fieldDescriptorProto =>
            `case ${fieldDescriptorProto.number}: { newProto.${memberName} = ${this.insideMessage.typescriptType.name}.decoders[${fieldDescriptorProto.number}].decode(reader, newProto.${memberName}?.field === "${fieldDescriptorProto.jsonName}" ? newProto.${memberName} : null); break; }`
        )
        .join("\n");
    }
    return `case ${this.field.fieldDescriptorProto.number}: { newProto.${this.field.fieldDescriptorProto.jsonName} = ${this.insideMessage.typescriptType.name}.decoders[${this.field.fieldDescriptorProto.number}].decode(reader, newProto.${this.field.fieldDescriptorProto.jsonName}); break; }`;
  }

  public setJsonObjectFields() {
    if (this.field.type === "oneof") {
      const name = this.field.name;
      return `if (!!this.${name}) {
  switch (this.${name}.field) {
${indent(
  this.field.fieldDescriptorProtos
    .map(
      fieldDescriptorProto =>
        `case "${fieldDescriptorProto.jsonName}": jsonObject.${
          fieldDescriptorProto.jsonName
        } = ${this.toJsonValue(fieldDescriptorProto, `this.${name}.value`)}; break;`
    )
    .join("\n"),
  2
)}
  }
}`;
    }
    return `if (${this.checkShouldSerialize(this.field.fieldDescriptorProto)}) { jsonObject.${
      this.field.fieldDescriptorProto.jsonName
    } = ${this.toJsonValue(
      this.field.fieldDescriptorProto,
      `this.${this.field.fieldDescriptorProto.jsonName}`
    )}; }`;
  }

  private checkShouldSerialize(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return `this.${fieldDescriptorProto.jsonName}.size > 0`;
      }
    }
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

  private serializerMethodName(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return "map";
      }
    }
    const typeString = google.protobuf.FieldDescriptorProto.Type[fieldDescriptorProto.type];
    return typeString.replace("TYPE_", "").toLowerCase();
  }

  private deserialize(fieldDescriptorProto: google.protobuf.IFieldDescriptorProto) {
    const deserializedValue = (() => {
      if (PACKABLE_TYPES.includes(fieldDescriptorProto.type)) {
        return `...buffer.${this.serializerMethodName(fieldDescriptorProto)}()`;
      }
      switch (fieldDescriptorProto.type) {
        case google.protobuf.FieldDescriptorProto.Type.TYPE_STRING:
          return "buffer.string()";
        case google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE:
          this.types.typescriptTypeFromProtobufType(
            fieldDescriptorProto.type,
            fieldDescriptorProto.typeName,
            this.insideMessage
          );
          return `${this.types.typescriptTypeFromProtobufType(
            fieldDescriptorProto.type,
            fieldDescriptorProto.typeName,
            this.insideMessage
          )}.deserialize(buffer.bytes())`;
        case google.protobuf.FieldDescriptorProto.Type.TYPE_GROUP:
          throw new Error("GROUP is unsupported.");
        case google.protobuf.FieldDescriptorProto.Type.TYPE_BYTES:
          return "buffer.bytes()";
        default:
          throw new Error(`Unrecognized field type: ${fieldDescriptorProto.type}`);
      }
    })();
    if (
      fieldDescriptorProto.label !== google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED &&
      PACKABLE_TYPES.includes(fieldDescriptorProto.type)
    ) {
      return `Deserializer.single([${deserializedValue}])`;
    }
    return deserializedValue;
  }

  private toJsonValue(
    fieldDescriptorProto: google.protobuf.IFieldDescriptorProto,
    valueVariable: string
  ): string {
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return `Array.from(${valueVariable}).reduce((currentVal: any, [key, val]) => {
          currentVal[String(key)] = ${this.toJsonValue(
            typeMetadata.protobufType.descriptorProto.field[1],
            "val"
          )};
          return currentVal;
        }, {})`;
      }
    }
    if (fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_ENUM) {
      const enumTypeName = this.types.typescriptTypeFromProtobufType(
        fieldDescriptorProto.type,
        fieldDescriptorProto.typeName,
        this.insideMessage
      );
      if (
        fieldDescriptorProto.label === google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED
      ) {
        return `${valueVariable}.map(value => ${enumTypeName}[value])`;
      }
      return `${enumTypeName}[${valueVariable}]`;
    }
    return `toJsonValue(${valueVariable})`;
  }
}

class Decoders {
  public static for(
    fieldDescriptorProto: google.protobuf.IFieldDescriptorProto,
    types: TypeRegistry,
    insideMessage: ITypeMetadata<IMessageDescriptor>,
    isOneof?: boolean
  ) {
    return new Decoders(fieldDescriptorProto, types, insideMessage, isOneof);
  }

  constructor(
    private readonly fieldDescriptorProto: google.protobuf.IFieldDescriptorProto,
    private readonly types: TypeRegistry,
    private readonly insideMessage: ITypeMetadata<IMessageDescriptor>,
    private readonly isOneof: boolean = false
  ) {}

  public instantiate(): string {
    if (this.isOneof) {
      return `Decoders.oneOfEntry("${this.fieldDescriptorProto.jsonName}", ${Decoders.for(
        this.fieldDescriptorProto,
        this.types,
        this.insideMessage
      ).instantiate()})`;
    }
    return `Decoders.${this.decodersMethodName()}(${this.decoderArguments()})${
      this.fieldDescriptorProto.label !== google.protobuf.FieldDescriptorProto.Label.LABEL_REPEATED
        ? ".single()"
        : ""
    }`;
  }

  private decodersMethodName() {
    if (this.fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(this.fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${this.fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return "map";
      }
    }
    const typeString = google.protobuf.FieldDescriptorProto.Type[this.fieldDescriptorProto.type];
    return typeString.replace("TYPE_", "").toLowerCase();
  }

  private decoderArguments(): string {
    if (this.fieldDescriptorProto.type === google.protobuf.FieldDescriptorProto.Type.TYPE_MESSAGE) {
      const typeMetadata = this.types.forFieldDescriptor(this.fieldDescriptorProto);
      if (typeMetadata.protobufType.isEnum === true) {
        throw new Error(
          `Lookup for message type ${this.fieldDescriptorProto.name} returned enum ${typeMetadata.protobufType.fullyQualifiedName}.`
        );
      }
      if (typeMetadata.protobufType.descriptorProto.options?.mapEntry) {
        return `${Decoders.for(
          typeMetadata.protobufType.fields[0],
          this.types,
          this.insideMessage
        ).instantiate()}, ${Decoders.for(
          typeMetadata.protobufType.fields[1],
          this.types,
          this.insideMessage
        ).instantiate()}`;
      }
      return `${this.types.typescriptTypeFromProtobufType(
        this.fieldDescriptorProto.type,
        this.fieldDescriptorProto.typeName,
        this.insideMessage
      )}.deserialize`;
    }
    return "";
  }
}

function indent(text: string, times: number = 1) {
  return text
    .split("\n")
    .map(line => `${"  ".repeat(times)}${line}`.trimRight())
    .join("\n");
}
