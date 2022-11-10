import { Flags } from "df/common/flags";
import { IHookHandler } from "df/testing";

// FlagOverridesFixture can be used to temporarily override flag values for testing purposes.
// At the end of the test run, any previous flag values will be restored.
export class FlagOverridesFixture {
  private originalArgs: Map<string, string>;

  constructor(setUp: IHookHandler, tearDown: IHookHandler) {
    setUp("reset snapshotted args", () => {
      this.originalArgs = new Map<string, string>();
    });
    tearDown("restore pre-test args", () => this.restoreArgs());
  }

  public set(flag: string, override: string) {
    if (!this.originalArgs.has(flag)) {
      this.originalArgs.set(flag, Flags.getRawFlagValue(flag));
    }
    Flags.setRawFlagValueForTesting(flag, override);
  }

  private restoreArgs() {
    for (const [flag, value] of Array.from(this.originalArgs.entries())) {
      Flags.setRawFlagValueForTesting(flag, value);
    }
  }
}
