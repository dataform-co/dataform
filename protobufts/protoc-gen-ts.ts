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

function getTypeFileMapping(fileDescriptorProtos: google.protobuf.IFileDescriptorProto[]) {
  const typeFileMapping = new Map<string, string>();

  const insertMappings = (
    filename: string,
    namespaceParts: string[],
    descriptorProtos: google.protobuf.IDescriptorProto[],
    enumDescriptorProtos: google.protobuf.IEnumDescriptorProto[]
  ) => {
    descriptorProtos.forEach(descriptorProto => {
      typeFileMapping.set(namespaceParts.concat(descriptorProto.name).join("."), filename);
      insertMappings(
        filename,
        namespaceParts.concat(descriptorProto.name),
        descriptorProto.nestedType,
        descriptorProto.enumType
      );
    });
    enumDescriptorProtos.forEach(enumDescriptorProto =>
      typeFileMapping.set(namespaceParts.concat(enumDescriptorProto.name).join("."), filename)
    );
  };

  fileDescriptorProtos.forEach(fileDescriptorProto =>
    insertMappings(
      fileDescriptorProto.name,
      fileDescriptorProto.package.split("."),
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
  fileTypeMapping: Map<string, string>,
  parameters: IGeneratorParameters
) {
  return `// AUTOMATICALLY GENERATED CODE. DO NOT EDIT.

// IMPORTS
${getImportLines(fileDescriptorProto.dependency, parameters.importPrefix).join("\n")}

// MESSAGES
${fileDescriptorProto.messageType.map(descriptorProto => getMessage(descriptorProto)).join("\n\n")}

// ENUMS
${fileDescriptorProto.enumType
  .map(enumDescriptorProto => getEnum(enumDescriptorProto))
  .join("\n\n")}
`;
}

function getImportLines(protoDependencies: string[], importPrefix?: string) {
  return protoDependencies.map(dependency => {
    let importPath = getGeneratedTypescriptFilename(dependency);
    if (importPrefix) {
      importPath = `${importPrefix}/${importPath}`;
    }
    return `import * as ${dependency
      .split("/")
      .slice(-1)[0]
      .split(".")
      .slice(0, -1)} from "${importPath}"`;
  });
}

function getMessage(descriptorProto: google.protobuf.IDescriptorProto, indentCount: number = 0) {
  return indent(
    `export class ${descriptorProto.name} {
${indent(
  descriptorProto.field
    .map(fieldDescriptorProto => `public ${fieldDescriptorProto.jsonName}: number = 0;`)
    .join("\n"),
  indentCount + 1
)}
}`,
    indentCount
  );
}

function getEnum(
  enumDescriptorProto: google.protobuf.IEnumDescriptorProto,
  indentCount: number = 0
) {
  return indent(
    `export enum ${enumDescriptorProto} {
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
    .map(line => `${"  ".repeat(indentCount)}${line}`)
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
