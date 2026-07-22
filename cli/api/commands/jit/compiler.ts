import { ChildProcess } from "child_process";
import * as path from "path";

import { BaseWorker } from "df/cli/api/commands/base_worker";
import { handleDbRequest } from "df/cli/api/commands/jit/rpc";
import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { IBigQueryExecutionOptions } from "df/cli/api/dbadapters/bigquery";
import { DEFAULT_COMPILATION_TIMEOUT_MILLIS } from "df/cli/api/utils/constants";
import { dataform } from "df/protos/ts";

export interface IJitWorkerMessage {
  type: "rpc_request" | "jit_response" | "jit_error";
  method?: string;
  request?: Uint8Array;
  correlationId?: string;
  response?: Uint8Array;
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
    timeoutMillis?: number,
    options?: IBigQueryExecutionOptions,
    onCancel?: (cancel: () => void) => void
  ): Promise<dataform.IJitCompilationResponse> {
    return await new JitCompileChildProcess().run(
      request,
      projectDir,
      dbadapter,
      dbclient,
      timeoutMillis || DEFAULT_COMPILATION_TIMEOUT_MILLIS,
      options,
      onCancel
    );
  }

  constructor() {
    super(path.resolve(__dirname, "../../../vm/jit_loader"));
  }

  private async run(
    request: dataform.IJitCompilationRequest,
    projectDir: string,
    dbadapter: IDbAdapter,
    dbclient: IDbClient,
    timeoutMillis: number,
    options: IBigQueryExecutionOptions | undefined,
    onCancel: ((cancel: () => void) => void) | undefined
  ): Promise<dataform.IJitCompilationResponse> {
    return await this.runWorker(
      timeoutMillis,
      child => {
        child.send({
          type: "jit_compile",
          request,
          projectDir
        });
      },
      async (message, child, resolve, reject) => {
        if (message.type === "rpc_request") {
          await this.handleRpcRequest(message, child, dbadapter, dbclient, options);
        } else if (message.type === "jit_response") {
          resolve(dataform.JitCompilationResponse.fromObject(message.response));
        } else if (message.type === "jit_error") {
          reject(new Error(message.error));
        }
      },
      onCancel
    );
  }

  private async handleRpcRequest(
    message: IJitWorkerMessage,
    child: ChildProcess,
    dbadapter: IDbAdapter,
    dbclient: IDbClient,
    options?: IBigQueryExecutionOptions
  ) {
    try {
      const response = await handleDbRequest(
        dbadapter,
        dbclient,
        message.method,
        message.request,
        options
      );
      child.send({
        type: "rpc_response",
        correlationId: message.correlationId,
        // Convert to plain array — child.send uses JSON IPC by default, which
        // serializes Uint8Array as {"0":n,...} and Buffer.from() rejects that.
        response: response ? Array.from(response) : response
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
