import { ExpressServer } from "df/server/express_server";
import { GrpcServer } from "df/server/grpc_server";
import * as yargs from "yargs";

interface IServerProps {
  httpPort: number;
  grpcPort: number;
  grpcProxyPort: number;
  serveApp?: boolean;
  projectDir: string;
}

export class DataformServer {
  private readonly props: IServerProps;

  private expressServer: ExpressServer;
  private grpcServer: GrpcServer;

  constructor(props: IServerProps) {
    this.props = {
      grpcPort: 9112,
      grpcProxyPort: 9111,
      httpPort: 9110,
      ...props
    };
  }

  public start() {
    if (this.props.serveApp) {
      this.expressServer = new ExpressServer({ ...this.props });
    }
    this.grpcServer = new GrpcServer({
      ...this.props
    });

    this.grpcServer.start();
    if (this.expressServer) {
      this.expressServer.start();
    }
  }

  public async shutdown() {
    if (this.expressServer) {
      this.expressServer.shutdown();
    }
    if (this.grpcServer) {
      await this.grpcServer.shutdown();
    }
  }
}

if (require.main === module) {
  const args = yargs
    .option("http-port", { default: 9110 })
    .option("grpc-proxy-port", { default: 9111 })
    .option("grpc-port", { default: 9112 })
    .option("serve-app", { type: "boolean", default: true })
    .option("project-dir", { type: "string", required: true }).argv;

  const server = new DataformServer({
    grpcPort: args["grpc-port"],
    grpcProxyPort: args["grpc-proxy-port"],
    httpPort: args["http-port"],
    projectDir: args["project-dir"],
    serveApp: args["serve-app"]
  });

  server.start();
}
