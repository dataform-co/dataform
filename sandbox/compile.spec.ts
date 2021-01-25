import { suite, test } from "df/testing";
import { ChildProcess, fork } from "child_process";
import { expect } from "chai";
import { coerceAsError } from "df/common/errors/errors";
import { dataform } from "df/protos/ts";

async function compile(compileExecutor: ChildProcess, compileConfig: dataform.ICompileConfig={}): Promise<string> {
    return new Promise<string>(async (resolve, reject) => {
        compileExecutor.on("error", (e: Error) => reject(coerceAsError(e)));

        compileExecutor.on("message", (messageOrError: string | Error) => {
          if (typeof messageOrError === "string") {
            resolve(messageOrError);
            return;
          }
          reject(coerceAsError(messageOrError));
        });

        compileExecutor.on("close", exitCode => {
          if (exitCode !== 0) {
            reject(new Error(`Compilation child process exited with exit code ${exitCode}.`));
          }
        });

        // Trigger the child process to start compiling.
        compileExecutor.send(compileConfig);
    });
    
}

suite("sandboxing", () => {
    test(`compilation`, async () => {

        // Should be:
        // /usr/local/google/home/eliaskassell/.cache/bazel/_bazel_eliaskassell/7a3b4b05af3e35677ea962500c529f6a/execroot/df/bazel-out/k8-py2-fastbuild/bin/sandbox/compile_executor
        // but currently the temporary linux sandbox is being used.
        const executorPath = require.resolve("./compile_executor");
        console.log("🚀 ~ file: compile.spec.ts ~ line 37 ~ test ~ executorPath", executorPath)

        const executorProcess = fork(require.resolve(executorPath), [], { stdio: [0, 1, 2, "ipc", "pipe"] });

        await compile(executorProcess);

        expect(true).to.equal(false);
    })
})
