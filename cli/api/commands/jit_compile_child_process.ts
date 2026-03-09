import { ChildProcess } from "child_process";
import { BaseWorker } from "df/cli/api/commands/base_worker";
import { handleRpc } from "df/cli/api/commands/jit_rpc";
import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { DEFAULT_COMPILATION_TIMEOUT_MILLIS } from "df/cli/api/utils/constants";
import { dataform } from "df/protos/ts";

export interface IJitWorkerMessage {
  type: "rpc_request" | "jit_response" | "jit_error";
  method?: string;
  request?: string;
  correlationId?: string;
  response?: any;
  error?: string;
}

export class JitCompileChildProcess extends BaseWorker<
  dataform.IJitCompilationResponse,
  IJitWorkerMessage
> {
  public static async compile(
    request: dataform.IJitCompilationRequest,
    projectDir: string,
    dbadapter: IDbAdapter,
    dbclient: IDbClient,
    timeoutMillis: number = DEFAULT_COMPILATION_TIMEOUT_MILLIS,
    dryRun?: boolean
  ): Promise<dataform.IJitCompilationResponse> {
    return await new JitCompileChildProcess().run(
      request,
      projectDir,
      dbadapter,
      dbclient,
      timeoutMillis,
      dryRun
    );
  }

  constructor() {
    super("../../vm/jit_loader");
  }

  private async run(
    request: dataform.IJitCompilationRequest,
    projectDir: string,
    dbadapter: IDbAdapter,
    dbclient: IDbClient,
    timeoutMillis: number,
    dryRun?: boolean
  ): Promise<dataform.IJitCompilationResponse> {
    return await this.runWorker(
      timeoutMillis,
      child => {
        child.send({
          type: "jit_compile",
          request: request instanceof dataform.JitCompilationRequest ? request.toJSON() : request,
          projectDir
        });
      },
      async (message, child, resolve, reject) => {
        if (message.type === "rpc_request") {
          await this.handleRpcRequest(message, child, dbadapter, dbclient, dryRun);
        } else if (message.type === "jit_response") {
          resolve(dataform.JitCompilationResponse.fromObject(message.response));
        } else if (message.type === "jit_error") {
          reject(new Error(message.error));
        }
      }
    );
  }

  private async handleRpcRequest(
    message: IJitWorkerMessage,
    child: ChildProcess,
    dbadapter: IDbAdapter,
    dbclient: IDbClient,
    dryRun?: boolean
  ) {
    try {
      const response = await handleRpc(
        dbadapter,
        dbclient,
        message.method,
        Buffer.from(message.request, "base64"),
        dryRun
      );
      child.send({
        type: "rpc_response",
        correlationId: message.correlationId,
        response: Buffer.from(response).toString("base64")
      });
    } catch (e) {
      child.send({
        type: "rpc_response",
        correlationId: message.correlationId,
        error: e.message
      });
    }
  }
}
