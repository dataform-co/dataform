import * as dfapi from "@dataform/api";
import { dataform as protos } from "@dataform/protos";
import { Service } from "@dataform/server/grpc_service";
import { writeFile } from "fs";
import * as grpc from "grpc";
import { join } from "path";
import { promisify } from "util";

export class ServiceImpl implements Service {
  constructor(private readonly projectDir: string) {}
  public async metadata(
    call: grpc.ServerUnaryCall<protos.server.IEmpty>
  ): Promise<protos.server.IMetadataResponse> {
    return {
      projectDir: this.projectDir
    };
  }
}
