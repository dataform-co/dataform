import * as os from "os";

export type TerminationSignal = "SIGINT" | "SIGTERM";

type SignalProcess = Pick<NodeJS.Process, "on" | "exit">;

const commandHandlers = new Map<TerminationSignal, () => void>();

/**
 * Lets the running command handle the first `signal` itself, e.g. `run` cancels the runner. A
 * second termination signal always exits. Pass `undefined` to exit on the first signal again.
 */
export function setSignalHandler(signal: TerminationSignal, handler: (() => void) | undefined) {
  if (handler) {
    commandHandlers.set(signal, handler);
  } else {
    commandHandlers.delete(signal);
  }
}

/**
 * Signals terminate Node without emitting "exit", so cleanup registered on "exit" (e.g. the
 * temporary directory of a dataformCoreVersion project) would not run. Exit explicitly with the
 * conventional 128 + signal number code instead, unless the command handles the first signal.
 */
export function handleTerminationSignals(proc: SignalProcess = process) {
  let signalled = false;
  (["SIGINT", "SIGTERM"] as const).forEach((signal) => {
    proc.on(signal, () => {
      const handler = commandHandlers.get(signal);
      if (handler && !signalled) {
        signalled = true;
        handler();
        return;
      }
      proc.exit(128 + os.constants.signals[signal]);
    });
  });
}
