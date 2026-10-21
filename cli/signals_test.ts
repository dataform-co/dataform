import { expect } from "chai";
import { EventEmitter } from "events";

import { handleTerminationSignals, setSignalHandler } from "df/cli/signals";
import { suite, test } from "df/testing";

suite("handleTerminationSignals", ({ afterEach }) => {
  afterEach("clear command handlers", () => {
    setSignalHandler("SIGINT", undefined);
    setSignalHandler("SIGTERM", undefined);
  });

  function createProcess() {
    const exitCodes: number[] = [];
    const proc = Object.assign(new EventEmitter(), {
      exit: (code: number) => {
        exitCodes.push(code);
      },
    });
    handleTerminationSignals(proc as any);
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

  test("exits even if another listener is registered", () => {
    const { proc, exitCodes } = createProcess();
    // E.g. a listener added by a library.
    proc.on("SIGINT", () => undefined);

    proc.emit("SIGINT");

    expect(exitCodes).deep.equals([130]);
  });

  test("lets the command handle the first signal and exits on the second", () => {
    const { proc, exitCodes } = createProcess();
    let handled = 0;
    setSignalHandler("SIGINT", () => {
      handled++;
    });

    proc.emit("SIGINT");
    expect(handled).equals(1);
    expect(exitCodes).deep.equals([]);

    proc.emit("SIGINT");
    expect(handled).equals(1);
    expect(exitCodes).deep.equals([130]);
  });
});
