import { ChildProcess, fork } from "child_process";

export abstract class BaseWorker<TResponse, TMessage = any> {
  protected constructor(private readonly loaderPath: string) {}

  protected async runWorker(
    timeoutMillis: number,
    onBoot: (child: ChildProcess) => void,
    onMessage: (message: TMessage, child: ChildProcess, resolve: (res: TResponse) => void, reject: (err: Error) => void) => void
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
        clearTimeout(timeout);
        child.kill("SIGKILL");
        fn();
      };

      const timeout = setTimeout(() => {
        terminate(() =>
          reject(new Error(`Worker timed out after ${timeoutMillis / 1000} seconds`))
        );
      }, timeoutMillis);

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
