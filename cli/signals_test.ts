import { expect } from "chai";
import { EventEmitter } from "events";

import { exitOnTerminationSignals } from "df/cli/signals";
import { suite, test } from "df/testing";

suite("exitOnTerminationSignals", () => {
  function createProcess() {
    const exitCodes: number[] = [];
    const proc = Object.assign(new EventEmitter(), {
      exit: (code: number) => {
        exitCodes.push(code);
      },
    });
    exitOnTerminationSignals(proc as any);
    return { proc, exitCodes };
  }

  test("exits with 130 on SIGINT", () => {
    const { proc, exitCodes } = createProcess();

    proc.emit("SIGINT");

    expect(exitCodes).deep.equals([130]);
  });

  test("exits with 143 on SIGTERM", () => {
    const { proc, exitCodes } = createProcess();

    proc.emit("SIGTERM");

    expect(exitCodes).deep.equals([143]);
  });

  test("leaves the signal to a command that handles it", () => {
    const { proc, exitCodes } = createProcess();
    let handledByCommand = false;
    proc.on("SIGINT", () => {
      handledByCommand = true;
    });

    proc.emit("SIGINT");

    expect(handledByCommand).equals(true);
    expect(exitCodes).deep.equals([]);
  });
});
