import * as os from "os";

type SignalProcess = Pick<NodeJS.Process, "on" | "listenerCount" | "exit">;

/**
 * Signals terminate Node without emitting "exit", so cleanup registered on "exit" (e.g. the
 * temporary directory of a dataformCoreVersion project) would not run. Exit explicitly with the
 * conventional 128 + signal number code instead, unless a command handles the signal itself.
 */
export function exitOnTerminationSignals(proc: SignalProcess = process) {
  (["SIGINT", "SIGTERM"] as const).forEach((signal) => {
    proc.on(signal, () => {
      if (proc.listenerCount(signal) === 1) {
        proc.exit(128 + os.constants.signals[signal]);
      }
    });
  });
}
