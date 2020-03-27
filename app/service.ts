import { rpcImpl } from "@dataform-tools/protobufjs/grpc_web_rpc_impl";
import { dataform } from "@dataform/protos";

export class Service extends dataform.server.Service {
  public static get(): Service {
    if (!Service.instance) {
      Service.instance = new Service();
    }
    return Service.instance;
  }
  private static instance: Service;

  constructor() {
    super(rpcImpl("http://localhost:9111", "dataform.server.Service"), false, false);
  }
}
