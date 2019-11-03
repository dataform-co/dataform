#!/usr/bin/env node
import { GrpcWebProxy, IGrpcWebProxyOptions } from "df/tools/grpc-web-proxy";
import * as fs from "fs";
import * as yargs from "yargs";

const argv = yargs
  .option("backend", { type: "string" })
  .option("port", { type: "number" })
  .option("secure", { type: "string" })
  .option("ssl-key-path", { type: "string" })
  .option("ssl-cert-path", { type: "string" }).argv;

const options: IGrpcWebProxyOptions = {
  backend: argv.backend,
  port: argv.port
};

if (argv.secure === "insecure") {
  options.secure = "insecure";
} else if (argv["ssl-key-path"]) {
  options.secure = {
    key: fs.readFileSync(argv["ssl-key-path"], "utf8"),
    cert: fs.readFileSync(argv["ssl-cert-path"], "utf8")
  };
} else {
  options.secure = "fake-https";
}

const _ = new GrpcWebProxy(options);
