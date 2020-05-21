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

  return {
    file: request.fileToGenerate.map(file => ({
      name: getGeneratedTypescriptFilename(file),
      content: getFileContent(fileDescriptorMapping.get(file), fileTypeMapping)
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

function getFileContent(
  fileDescriptorProto: google.protobuf.IFileDescriptorProto,
  fileTypeMapping: Map<string, string>
) {
  return `// AUTOMATICALLY GENERATED CODE.

// IMPORTS
${getImportLines(fileDescriptorProto.dependency).join("\n")}
`;
}

function getImportLines(protoDependencies: string[]) {
  return protoDependencies.map(
    dependency =>
      `import * as ${dependency
        .split("/")
        .slice(-1)[0]
        .split(".")
        .slice(0, -1)} from "${getGeneratedTypescriptFilename(dependency)}"`
  );
}

function getGeneratedTypescriptFilename(protoFilename: string) {
  return protoFilename.replace(".proto", ".ts");
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
