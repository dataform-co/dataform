import { fail } from "assert";
import { assert, expect } from "chai";

import { runWithTimeout, sleep } from "df/common/promises";
import { suite, test } from "df/testing";

suite(__filename, () => {
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
