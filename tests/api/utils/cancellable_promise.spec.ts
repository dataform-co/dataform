import { CancellablePromise } from "@dataform/api/utils/cancellable_promise";
import { expect } from "chai";

describe("@dataform/api/utils/cancellable_promise", () => {
  it("cancel_is_called", () => {
    let wasCancelled = false;
    const promise = new CancellablePromise((resolve, reject, onCancel) => {
      onCancel(() => {
        wasCancelled = true;
      });
    });
    promise.cancel();
    expect(wasCancelled).is.true;
  });

  it("resolves", async () => {
    const result = await new CancellablePromise<string>((resolve, reject, onCancel) => {
      resolve("resolved");
    });
    expect(result).equals("resolved");
  });
});
