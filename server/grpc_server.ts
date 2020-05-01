import { GrpcWebProxy } from "df/grpc-web-proxy";
import * as Service from "df/server/grpc_service";
import { ServiceImpl } from "df/server/service_impl";
import * as grpc from "grpc";

interface IProps {
  grpcPort: number;
  grpcProxyPort: number;
  projectDir: string;
}

export class GrpcServer {
  private readonly server: grpc.Server;
  private grpcProxy: GrpcWebProxy;

  public constructor(private props: IProps) {
    this.server = new grpc.Server();
  }

  public start() {
    this.server.addService(
      Service.DEFINITION,
      new Service.ServicePromiseWrapper(
        new ServiceImpl(this.props.projectDir)
      )
    );

    this.server.bind(`0.0.0.0:${this.props.grpcPort}`, grpc.ServerCredentials.createInsecure());
    this.server.start();

    // Also start the gRPC web proxy and serve the client application.

    this.grpcProxy = new GrpcWebProxy({
      backend: `http://localhost:${this.props.grpcPort}`,
      port: this.props.grpcProxyPort
    });
  }

  public async shutdown() {
    this.server.forceShutdown();
    this.grpcProxy.shutdown();
  }
}
