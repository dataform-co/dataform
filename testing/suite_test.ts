import { expect } from "chai";
import { ISuiteContext, Runner, suite, test } from "df/testing";

Runner.setNoExit(true);

class ExampleFixture {
  public counter = 0;
  public register(ctx: ISuiteContext) {
    ctx.setUp(() => {
      this.counter = 1;
    });
    ctx.tearDown(() => {
      this.counter = 2;
    });
  }
}

const _ = (async () => {
  const exampleFixture = new ExampleFixture();
  suite("suite", () => {
    test("passes", async () => true);
    test("fails on throw", () => {
      throw new Error("fail-sync");
    });
    test("fails on promise rejection", async () => {
      await Promise.reject(new Error("fail-async"));
    });
    test(
      "times out",
      { timeout: 10 },
      async () => await new Promise(resolve => setTimeout(resolve, 100000))
    );
    suite("with before and after", ({ beforeEach, afterEach }) => {
      let counter = 0;
      beforeEach(() => {
        counter += 1;
      });
      afterEach(() => {
        counter = 0;
      });
      test("passes on first test", () => {
        expect(counter).equals(1);
        counter = 2;
      });
      test("passes on second test", () => {
        expect(counter).equals(1);
        counter = 2;
      });
    });

    suite("with set up and tear down", ctx => {
      exampleFixture.register(ctx);
      test({ name: "set up is called" }, () => {
        expect(exampleFixture.counter).equals(1);
      });
    });

    suite("can execute in parallel", { parallel: true }, async () => {
      let counter = 0;
      test({ name: "test1" }, async () => {
        expect(counter).equals(0);
        await new Promise(resolve => setTimeout(resolve, 10));
        counter = 1;
      });
      test({ name: "test2" }, async () => {
        expect(counter).equals(0);
        await new Promise(resolve => setTimeout(resolve, 10));
        counter = 1;
      });
    });
  });

  const results = await Runner.result();

  // Override the test exit code behaviour.
  process.exitCode = 0;

  // Clean up the rest results.
  const resultsClean = results.map(result => {
    const newResult = { ...result };
    if (result.err) {
      newResult.err = result.err.message;
    }
    if (result.hasOwnProperty("duration")) {
      delete newResult.duration;
    }
    return newResult;
  });

  try {
    expect(resultsClean).deep.members([
      { path: ["suite", "passes"], outcome: "passed" },
      { path: ["suite", "fails on throw"], outcome: "failed", err: "fail-sync" },
      { path: ["suite", "fails on promise rejection"], outcome: "failed", err: "fail-async" },
      { path: ["suite", "times out"], outcome: "timeout", err: "Timed out (10ms)." },
      { path: ["suite", "with before and after", "passes on first test"], outcome: "passed" },
      { path: ["suite", "with before and after", "passes on second test"], outcome: "passed" },
      { path: ["suite", "with set up and tear down", "set up is called"], outcome: "passed" },
      { path: ["suite", "can execute in parallel", "test1"], outcome: "passed" },
      { path: ["suite", "can execute in parallel", "test2"], outcome: "passed" }
    ]);

    // Tear down should have been called.
    expect(exampleFixture.counter).equals(2);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
  process.exit(0);
})();
