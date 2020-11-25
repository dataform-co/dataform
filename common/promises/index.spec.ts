import { fail } from "assert";
import { assert, expect } from "chai";

import { retry, runWithTimeout, sleep } from "df/common/promises";
import { suite, test } from "df/testing";

suite(__filename, () => {
  suite("runWithTimeout", () => {
    test("returns the result of the original function", async () => {
      const result = await runWithTimeout(
        async () => "hello",
        async () => {
          throw new Error("timed out!");
        },
        1000
      );
      expect(result).eql("hello");
    });

    test("times out and returns result of timeout function", async () => {
      const result = await runWithTimeout(
        async () => {
          let timer: NodeJS.Timer;
          try {
            return new Promise<string>(resolve => {
              timer = setTimeout(() => resolve("wrong!"), 10000);
            });
          } finally {
            clearTimeout(timer);
          }
        },
        async () => "hello",
        1000
      );
      expect(result).eql("hello");
    });

    test("times out and timeout function throws", async () => {
      try {
        await runWithTimeout(
          async () => {
            let timer: NodeJS.Timer;
            try {
              return new Promise<string>(resolve => {
                timer = setTimeout(() => resolve("wrong!"), 10000);
              });
            } finally {
              clearTimeout(timer);
            }
          },
          async () => {
            throw new Error("the function timed out");
          },
          1000
        );
        assert.fail("Expected timeout to fire, and Error to be thrown.");
      } catch (e) {
        expect(e.message).eql("the function timed out");
      }
    });

    test("handles exception in promise initializer", async () => {
      try {
        await runWithTimeout(
          // Won't return in time.
          async () => await sleep(1000),
          // Throw a non async error.
          () => {
            throw new Error("initialization error!");
          },
          100
        );
      } catch (e) {
        expect(e.message).equals("initialization error!");
        return;
      }
      fail("Expected to catch an error.");
    });
  });

  suite("retry", () => {
    test("doesn't retry if the function succeeds", async () => {
      let calledTimes = 0;
      const succeedingFunc = async () => {
        calledTimes += 1;
        return "success";
      };
      const result = await retry(succeedingFunc, 3);
      expect(result).to.eq("success");
      expect(calledTimes).eq(1);
    });

    test("doesn't retry if the function fails and attempts is 1", async () => {
      let calledTimes = 0;
      const failingFunc = async () => {
        calledTimes += 1;
        throw new Error("an error");
      };
      try {
        await retry(failingFunc);
      } catch (e) {
        expect(e.toString()).to.eq("Error: an error");
      }
      expect(calledTimes).eq(1);
    });

    test("calls the function three times if the function fails and attempts is 3", async () => {
      let calledTimes = 0;
      const failingFunc = async () => {
        calledTimes += 1;
        throw new Error("an error");
      };
      try {
        await retry(failingFunc, 3);
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
        await retry(failingFunc, 3);
      } catch (e) {
        expect(e.message).equal("an error");
      }
      expect(calledTimes).eq(2);
    });
  });
});
