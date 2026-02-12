import * as $protobuf from "protobufjs";

import { JitOperationResult } from "df/core/actions/operation";
import { JitTableResult } from "df/core/actions/table";
import { IActionContext, ITableContext, JitContext } from "df/core/contextables";
import { IncrementalTableJitContext, SqlActionJitContext, TableJitContext } from "df/core/jit_context";
import { dataform } from "df/protos/ts";

function makeMainBody<Context, T>(code: string): (jctx: JitContext<Context>) => Promise<T> {
  return (
    jctx => {
      // tslint:disable-next-line: tsr-detect-eval-with-expression
      const body = new Function(
        "jctx", `const mainAsync = ${code};\nreturn mainAsync(jctx);`
      ) as (jctx: JitContext<Context>) => Promise<T>;
      return body(jctx);
    });
}

function makeJitTableResult(result: JitTableResult): dataform.IJitTableResult {
  let jitResult: dataform.IJitTableResult = {};
  if (typeof result === "string") {
    jitResult.query = result;
  } else {
    jitResult = result;
  }

  return dataform.JitTableResult.create(jitResult);
}

function jitCompileOperation(
  request: dataform.IJitCompilationRequest,
  adapter: dataform.DbAdapter,
): Promise<dataform.IJitOperationResult> {
  const mainBody = makeMainBody<IActionContext, JitOperationResult>(request.jitCode);

  const jctx: JitContext<IActionContext> = new SqlActionJitContext(
    adapter, request,
  );
  return mainBody(jctx).then(mainResult => {
    let queries: string[] | null = [];
    if (typeof mainResult === "string") {
      queries.push(mainResult);
    } else if (Array.isArray(mainResult)) {
      queries.push(...mainResult);
    } else {
      queries = mainResult.queries;
    }

    return dataform.JitOperationResult.create({ queries });
  });
}

function jitCompileTable(
  request: dataform.IJitCompilationRequest,
  adapter: dataform.DbAdapter,
): Promise<dataform.IJitTableResult> {
  const mainBody = makeMainBody<ITableContext, JitTableResult>(request.jitCode);

  const jctx: JitContext<ITableContext> = new TableJitContext(
    adapter, request,
  );
  return mainBody(jctx).then(makeJitTableResult);
}

function jitCompileIncrementalTable(
  request: dataform.IJitCompilationRequest,
  adapter: dataform.DbAdapter,
): Promise<dataform.IJitIncrementalTableResult> {
  const mainBody = makeMainBody<ITableContext, JitTableResult>(request.jitCode);

  const incrementalJctx = new IncrementalTableJitContext(
    adapter, request, true,
  );
  const regularJctx = new IncrementalTableJitContext(
    adapter, request, false,
  );

  return Promise.all([
    mainBody(incrementalJctx),
    mainBody(regularJctx),
  ]).then(([incrementalResult, regularResult]) => {
    return dataform.JitIncrementalTableResult.create({
      incremental: makeJitTableResult(incrementalResult),
      regular: makeJitTableResult(regularResult),
    });
  });
}

export interface IJitCompiler {
  compile: (request: Uint8Array) => Promise<Uint8Array>;
}

/** RPC callback, implementing DbAdapter. */
export type RpcCallback = (method: string, request: Uint8Array, callback: (error: Error | null, response: Uint8Array) => void) => void;

export function jitCompile(request: dataform.IJitCompilationRequest, rpcCallback: RpcCallback): Promise<dataform.IJitCompilationResponse> {
  const rpcImpl: $protobuf.RPCImpl = (method, internalRequest, callback) => {
    rpcCallback(method.name, internalRequest, callback);
  };
  const dbAdapter = dataform.DbAdapter.create(rpcImpl);

  switch (request.compilationTargetType) {
    case dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION:
      return jitCompileOperation(request, dbAdapter).then(
        operation => dataform.JitCompilationResponse.create({ operation }));
    case dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_TABLE:
      return jitCompileTable(request, dbAdapter).then(
        table => dataform.JitCompilationResponse.create({ table }));
    case dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_INCREMENTAL_TABLE:
      return jitCompileIncrementalTable(request, dbAdapter).then(
        incrementalTable => dataform.JitCompilationResponse.create({ incrementalTable }));
    default:
      throw new Error(`Unrecognized compilation target type: ${request.compilationTargetType}`);
  }
}

/** Main entry point for the JiT compiler. */
export function jitCompiler(rpcCallback: RpcCallback): IJitCompiler {
  return {
    compile: (request: Uint8Array) => {
      const requestMessage = dataform.JitCompilationRequest.decode(request);
      return jitCompile(requestMessage, rpcCallback).then(
        response => dataform.JitCompilationResponse.encode(response).finish()
      );
    }
  };
}
