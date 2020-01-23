import { CancellablePromise } from "@dataform/api/utils/cancellable_promise";
import { expect } from "chai";
import { suite, test } from "@dataform/testing";

suite("cancellable_promise", () => {
  test("cancel is called", () => {
    let wasCancelled = false;
    const promise = new CancellablePromise((resolve, reject, onCancel) => {
      onCancel(() => {
        wasCancelled = true;
      });
    });
    promise.cancel();
    expect(wasCancelled).is.true;
  });

  test("cancel called early", async () => {
    let wasCancelled = false;
    const promise = new CancellablePromise((resolve, reject, onCancel) => {
      setTimeout(
        () =>
          onCancel(() => {
            wasCancelled = true;
            resolve();
          }),
        10
      );
    });
    promise.cancel();
    await promise;
    expect(wasCancelled).is.true;
  });

  test("resolves", async () => {
    const result = await new CancellablePromise<string>((resolve, reject, onCancel) => {
      resolve("resolved");
    });
    expect(result).equals("resolved");
  });
});
