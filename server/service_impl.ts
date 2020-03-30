import { dataform as protos } from "@dataform/protos";
import { IService } from "@dataform/server/grpc_service";
import * as grpc from "grpc";

export class ServiceImpl implements IService {
  constructor(private readonly projectDir: string) {}
  public async metadata(
    call: grpc.ServerUnaryCall<protos.server.IEmpty>
  ): Promise<protos.server.IMetadataResponse> {
    return {
      projectDir: this.projectDir
    };
  }
}
