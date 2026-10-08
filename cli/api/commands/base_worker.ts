import { ChildProcess, fork } from "child_process";
import * as path from "path";

// Worker entry point used when there is no worker_bundle.js next to the running code, i.e. in
// dev (`bazel run`) and tests. It runs the same cli/vm/worker.ts as the bundle.
const DEV_WORKER_LOADER_PATH = path.resolve(__dirname, "../../vm/worker_loader");

export abstract class BaseWorker<TResponse, TMessage = any> {
  protected async runWorker(
    timeoutMillis: number,
    onBoot: (child: ChildProcess) => void,
    onMessage: (
      message: TMessage,
      child: ChildProcess,
      resolve: (res: TResponse) => void,
      reject: (err: Error) => void,
    ) => void,
    onCancel?: (cancel: () => void) => void,
  ): Promise<TResponse> {
    const forkScript = this.resolveScript();
    const child = fork(forkScript, [], {
      stdio: [0, 1, 2, "ipc", "pipe"],
    });

    return new Promise((resolve, reject) => {
      let completed = false;
      let booted = false;

      const terminate = (fn: () => void) => {
        if (completed) {
          return;
        }
        completed = true;
        clearTimeout(timeout);
        child.kill("SIGKILL");
        fn();
      };

      const timeout = setTimeout(() => {
        terminate(() =>
          reject(
            new Error(
              `Compilation timed out after ${timeoutMillis / 1000} seconds. ` +
                `To allow more time, re-run with a longer --timeout ` +
                `(e.g. --timeout=2m, --timeout=1h).`,
            ),
          ),
        );
      }, timeoutMillis);

      onCancel?.(() =>
        terminate(() => reject(new Error("Run cancelled while worker was in flight."))),
      );

      child.on("message", (message: any) => {
        if (message.type === "worker_booted") {
          if (!booted) {
            booted = true;
            onBoot(child);
          }
          return;
        }
        onMessage(
          message,
          child,
          (res) => terminate(() => resolve(res)),
          (err) => terminate(() => reject(err)),
        );
      });

      child.on("error", (err) => {
        terminate(() => reject(err));
      });

      child.on("exit", (code, signal) => {
        if (!completed) {
          const errorMsg =
            code !== 0 && code !== null
              ? `Worker exited with code ${code} and signal ${signal}`
              : "Worker exited without sending a response message";
          terminate(() => reject(new Error(errorMsg)));
        }
      });
    });
  }

  private resolveScript() {
    const pathsToTry = ["./worker_bundle.js", DEV_WORKER_LOADER_PATH];
    for (const p of pathsToTry) {
      try {
        return require.resolve(p);
      } catch (e) {
        // Continue to next path.
      }
    }
    throw new Error(`Could not resolve worker script. Tried: ${pathsToTry.join(", ")}`);
  }
}
