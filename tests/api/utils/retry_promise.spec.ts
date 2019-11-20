import { retryPromise } from "@dataform/api/utils/retry_promise";
import { expect } from "chai";

describe("retryPromise", () => {
  it("doesn't retry if the function succeeds", async () => {
    let calledTimes = 0;
    const succeedingFunc = async () => {
      calledTimes += 1;
      return "success";
    };
    const result = await retryPromise(succeedingFunc, 2);
    expect(result).to.eq("success");
    expect(calledTimes).eq(1);
  });

  it("doesn't retry if the function fails and retries is 0", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retryPromise(failingFunc, 0);
    } catch (e) {
      expect(e.toString()).to.eq("Error: an error");
    }
    expect(calledTimes).eq(1);
  });

  it("calls the function three times if the function fails and retries is 2", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retryPromise(failingFunc, 2);
    } catch (e) {}
    expect(calledTimes).eq(3);
  });

  it("will eventually return a success if the function has failed before", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      if (calledTimes > 1) {
        return "success";
      }
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retryPromise(failingFunc, 2);
    } catch (e) {}
    expect(calledTimes).eq(2);
  });
});
