import * as fs from "fs";
import * as path from "path";
import * as net from "net";
import { promisify } from "util";
import * as os from "os";

import { ChildProcess, fork, spawn } from "child_process";
import deepmerge from "deepmerge";
import { validWarehouses } from "df/api/dbadapters";
import { coerceAsError, ErrorWithCause } from "df/common/errors/errors";
import { decode64, encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { v4 as uuid } from "uuid";

// Project config properties that are required.
const mandatoryProps: Array<keyof dataform.IProjectConfig> = ["warehouse", "defaultSchema"];

// Project config properties that require alphanumeric characters, hyphens or underscores.
const simpleCheckProps: Array<keyof dataform.IProjectConfig> = [
  "assertionSchema",
  "databaseSuffix",
  "schemaSuffix",
  "tablePrefix",
  "defaultSchema"
];

export class CompilationTimeoutError extends Error { }

export async function compile(
  compileConfig: dataform.ICompileConfig = {},
  useSandbox2?: boolean,
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  compileConfig = { ...compileConfig, projectDir: path.resolve(compileConfig.projectDir) }

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    const projectConfig = JSON.parse(dataformJson);
    checkDataformJsonValidity(deepmerge(projectConfig, compileConfig.projectConfigOverride || {}));
  } catch (e) {
    throw new ErrorWithCause(
      `Compilation failed. ProjectConfig ('dataform.json') is invalid: ${e.message}`,
      e
    );
  }

  var result: string = "";

  const socketPath = `/tmp/${uuid()}.sock`;

  if (fs.existsSync(socketPath)) {
    fs.unlinkSync(socketPath);
  }
  const server = net.createServer((socket) => {
    socket.on("data", (buf) => {
      result += buf.toString();
    });
  });

  server.listen(socketPath);

  await CompileChildProcess.forkProcess(socketPath, { ...compileConfig, useMain: false }, useSandbox2).timeout(compileConfig.timeoutMillis || 5000);

  await promisify(server.close.bind(server))();

  if (result.startsWith("ERROR:")) {
    throw coerceAsError(JSON.parse(result.substring(6)));
  }
  const decodedResult = decode64(dataform.CompiledGraph, result);
  return decodedResult;
}

export class CompileChildProcess {
  public static forkProcess(socket: string, compileConfig: dataform.ICompileConfig, useSandbox2: boolean) {
    const platformPath = os.platform() === "darwin" ? "nodejs_darwin_amd64" : "nodejs_linux_amd64";
    const nodePath = path.join(process.env.RUNFILES, "df", `external/${platformPath}/bin/nodejs/bin/node`);
    const workerRootPath = path.join(process.env.RUNFILES, "df", "sandbox/worker");
    const sandboxerPath = path.join(process.env.RUNFILES, "df", `sandbox/compile_executor`);
    if (useSandbox2) {
      return new CompileChildProcess(
        spawn(sandboxerPath, [nodePath, workerRootPath, socket, encode64(dataform.CompileConfig, compileConfig), compileConfig.projectDir], { stdio: [0, 1, 2, "ipc", "pipe"] })
      );
    } else {
      return new CompileChildProcess(
        spawn(nodePath, [path.join(workerRootPath, "worker_bundle.js"), socket, encode64(dataform.CompileConfig, compileConfig)], { stdio: [0, 1, 2, "ipc", "pipe"] })
      );
    }
  }
  private readonly childProcess: ChildProcess;

  constructor(childProcess: ChildProcess) {
    this.childProcess = childProcess;
  }

  public async timeout(timeoutMillis: number) {
    const compileInChildProcess = new Promise<string>(async (resolve, reject) => {
      this.childProcess.on("exit", exitCode => {
        if (exitCode !== 0) {
          reject(new Error(`Compilation child process exited with exit code ${exitCode}.`));
        }
        resolve("Compilation completed successfully");
      });
    });
    let timer;
    const timeout = new Promise(
      (resolve, reject) =>
      (timer = setTimeout(
        () => reject(new CompilationTimeoutError("Compilation timed out")),
        timeoutMillis
      ))
    );
    try {
      await Promise.race([timeout, compileInChildProcess]);
      return await compileInChildProcess;
    } finally {
      if (!this.childProcess.killed) {
        this.childProcess.kill("SIGKILL");
      }
      if (timer) {
        clearTimeout(timer);
      }
    }
  }
}

export const checkDataformJsonValidity = (dataformJsonParsed: { [prop: string]: any }) => {
  const invalidWarehouseProp = () => {
    return dataformJsonParsed.warehouse && !validWarehouses.includes(dataformJsonParsed.warehouse)
      ? `Invalid value on property warehouse: ${dataformJsonParsed.warehouse
      }. Should be one of: ${validWarehouses.join(", ")}.`
      : null;
  };
  const invalidProp = () => {
    const invProp = simpleCheckProps.find(prop => {
      return prop in dataformJsonParsed && !/^[a-zA-Z_0-9\-]*$/.test(dataformJsonParsed[prop]);
    });
    return invProp
      ? `Invalid value on property ${invProp}: ${dataformJsonParsed[invProp]}. Should only contain alphanumeric characters, underscores and/or hyphens.`
      : null;
  };
  const missingMandatoryProp = () => {
    const missMandatoryProp = mandatoryProps.find(prop => {
      return !(prop in dataformJsonParsed);
    });
    return missMandatoryProp ? `Missing mandatory property: ${missMandatoryProp}.` : null;
  };
  const message = invalidWarehouseProp() || invalidProp() || missingMandatoryProp();
  if (message) {
    throw new Error(message);
  }
};
