import { ChildProcess, fork } from "child_process";

export abstract class BaseWorker<TResponse, TMessage = any> {
  protected constructor(private readonly loaderPath: string) {}

  protected async runWorker(
    timeoutMillis: number | undefined,
    onBoot: (child: ChildProcess) => void,
    onMessage: (message: TMessage, child: ChildProcess, resolve: (res: TResponse) => void, reject: (err: Error) => void) => void,
    onCancel?: (cancel: () => void) => void
  ): Promise<TResponse> {
    const forkScript = this.resolveScript();
    const child = fork(forkScript, [], {
      stdio: [0, 1, 2, "ipc", "pipe"]
    });

    return new Promise((resolve, reject) => {
      let completed = false;
      let booted = false;

      const terminate = (fn: () => void) => {
        if (completed) {
          return;
        }
        completed = true;
        if (timeout !== undefined) {
          clearTimeout(timeout);
        }
        child.kill("SIGKILL");
        fn();
      };

      const timeout = timeoutMillis !== undefined
        ? setTimeout(() => {
            terminate(() =>
              reject(new Error(
                `Compilation timed out after ${timeoutMillis / 1000} seconds. ` +
                `To allow more time, re-run with a longer --timeout ` +
                `(e.g. --timeout=2m, --timeout=1h).`
              ))
            );
          }, timeoutMillis)
        : undefined;

      onCancel?.(() =>
        terminate(() => reject(new Error("Run cancelled while worker was in flight.")))
      );

      child.on("message", (message: any) => {
        if (message.type === "worker_booted") {
          if (!booted) {
            booted = true;
            onBoot(child);
          }
          return;
        }
        onMessage(message, child, (res) => terminate(() => resolve(res)), (err) => terminate(() => reject(err)));
      });

      child.on("error", err => {
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
    const pathsToTry = ["./worker_bundle.js", this.loaderPath];
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
