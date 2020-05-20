import { google } from "df/protobufts/ts-protoc-protos";
import * as fs from "fs";

const request = google.protobuf.compiler.CodeGeneratorRequest.decode(fs.readFileSync("/dev/stdin"));

const response = google.protobuf.compiler.CodeGeneratorResponse.create({
  file: [
    {
      name: "protos/dataform_ts_proto",
      content: "content: " + request.fileToGenerate.join(",")
    }
  ]
});

const buf = Buffer.from(google.protobuf.compiler.CodeGeneratorResponse.encode(response).finish());

process.stdout.write(buf);
