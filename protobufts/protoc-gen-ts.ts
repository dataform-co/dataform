import * as fs from "fs";

import { parseGeneratorParameters } from "df/protobufts/parameters";
import { FileTranspiler } from "df/protobufts/transpiler";
import { google } from "df/protobufts/ts-protoc-protos";
import { TypeRegistry } from "df/protobufts/types";

function generateFiles(
  request: google.protobuf.compiler.ICodeGeneratorRequest
): google.protobuf.compiler.ICodeGeneratorResponse {
  const typeRegistry = TypeRegistry.fromFiles(request.protoFile);
  const parameters = parseGeneratorParameters(request.parameter);
  return {
    file: request.fileToGenerate.map(filename =>
      FileTranspiler.forProtobufFile(
        request.protoFile.find(protoFile => protoFile.name === filename),
        typeRegistry,
        parameters
      ).generateFileContent()
    )
  };
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
