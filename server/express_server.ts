import * as express from "express";
import * as http from "http";
import { resolve } from "path";

export interface IProps {
  httpPort: number;
}

export class ExpressServer {
  private httpServer: http.Server;

  constructor(private props: IProps) {}
  public start() {
    const expressServer = express();
    expressServer.use("/", express.static(resolve(__dirname, "..", "app")));
    expressServer.get("*", (req, res) => {
      res.sendFile(resolve(__dirname, "..", "app", "index.html"));
    });
    this.httpServer = expressServer.listen(this.props.httpPort);
  }

  public shutdown() {
    this.httpServer.close();
  }
}
