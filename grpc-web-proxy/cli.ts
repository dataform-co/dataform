import { GrpcWebProxy, IGrpcWebProxyOptions, Mode } from "df/grpc-web-proxy";
import * as fs from "fs";
import * as yargs from "yargs";

const argv = yargs
  .option("backend", {
    type: "string",
    describe: "URL to the backend such as http://localhost:8000"
  })
  .option("port", { type: "number" })
  .option("mode", { type: "string", choices: [...Mode], default: "http1-insecure" })
  .option("ssl-key-path", { type: "string" })
  .option("ssl-cert-path", { type: "string" }).argv;

const options: IGrpcWebProxyOptions = {
  backend: argv.backend,
  port: argv.port
};

if (argv.mode) {
  options.mode = argv.mode as any;
}

if (argv["ssl-key-path"]) {
  // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
  options.key = fs.readFileSync(argv["ssl-key-path"], "utf8");
  // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
  options.cert = fs.readFileSync(argv["ssl-cert-path"], "utf8");
}

const _ = new GrpcWebProxy(options);
