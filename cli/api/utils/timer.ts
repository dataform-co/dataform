import Long from "long";

import { dataform } from "df/protos/ts";

export class Timer {
  public static start(existingTiming?: dataform.ITiming) {
    return new Timer(existingTiming?.startTimeMillis?.toNumber() || new Date().valueOf());
  }
  private constructor(readonly startTimeMillis: number) {}

  public current(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
    };
  }

  public end(): dataform.ITiming {
    return {
      startTimeMillis: Long.fromNumber(this.startTimeMillis),
      endTimeMillis: Long.fromNumber(new Date().valueOf()),
    };
  }
}
