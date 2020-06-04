import { expect } from "chai";

import { retry } from "df/api/utils/retry";
import { suite, test } from "df/testing";

suite("retry", () => {
  test("doesn't retry if the function succeeds", async () => {
    let calledTimes = 0;
    const succeedingFunc = async () => {
      calledTimes += 1;
      return "success";
    };
    const result = await retry(succeedingFunc, 2);
    expect(result).to.eq("success");
    expect(calledTimes).eq(1);
  });

  test("doesn't retry if the function fails and retries is 0", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retry(failingFunc, 0);
    } catch (e) {
      expect(e.toString()).to.eq("Error: an error");
    }
    expect(calledTimes).eq(1);
  });

  test("calls the function three times if the function fails and retries is 2", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retry(failingFunc, 2);
    } catch (e) {
      expect(e.message).equal("an error");
    }
    expect(calledTimes).eq(3);
  });

  test("will eventually return a success if the function has failed before", async () => {
    let calledTimes = 0;
    const failingFunc = async () => {
      if (calledTimes > 1) {
        return "success";
      }
      calledTimes += 1;
      throw new Error("an error");
    };
    try {
      await retry(failingFunc, 2);
    } catch (e) {
      expect(e.message).equal("an error");
    }
    expect(calledTimes).eq(2);
  });
});
