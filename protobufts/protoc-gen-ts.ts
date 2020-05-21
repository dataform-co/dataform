import { google } from "df/protobufts/ts-protoc-protos";
import * as fs from "fs";

const request = google.protobuf.compiler.CodeGeneratorRequest.decode(fs.readFileSync("/dev/stdin"));

const response = google.protobuf.compiler.CodeGeneratorResponse.create({
  file: request.fileToGenerate.map(file => ({
    name: file.replace(".proto", ".ts"),
    content:
      "fileToGenerate: " +
      request.fileToGenerate.join(",") +
      " protoFile: " +
      request.protoFile.map(f => f.name).join(",")
  }))
});

const buf = Buffer.from(google.protobuf.compiler.CodeGeneratorResponse.encode(response).finish());

process.stdout.write(buf);
