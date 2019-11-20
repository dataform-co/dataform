import { retryPromise } from "@dataform/api/utils/retry_promise";
import { expect } from "chai";

describe("retryPromise", () => {
  it("doesn't retry if the function succeeds", async () => {
    let calledTimes = 0;
    const succeedingFunc = async () => {
      calledTimes += 1;
      return "success";
    };
    await retryPromise(succeedingFunc, 2);
    expect(calledTimes).eq(1);
  });

  it("doesn't retry if the function fails and retries is 0", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      return new Error("error");
    };
    try {
      await retryPromise(failingFunc, 0);
    } catch (e) {
      expect(calledTimes).eq(1);
    }
  });

  it("retries thrice if the function fails and retries is 3", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      return new Error("error");
    };
    try {
      await retryPromise(failingFunc, 3);
    } catch (e) {
      expect(calledTimes).eq(3);
    }
  });
});
