import { Bootstrap, Reducers } from "@dataform/packages/bootstrap";
import alea from "alea";
import { expect } from "chai";
import "reflect-metadata";

describe("bootstrap", () => {
  it("uniform distribution", () => {
    console.log(JSON.stringify(Reflect.getMetadata("design:paramtypes", Bootstrap, "estimate")));

    // It's a good idea to run these tests a few times.
    for (let iteration = 0; iteration < 5; iteration++) {
      const testValues: number[] = [];
      const rng = alea(`uniform_${iteration}`);
      for (let i = 0; i <= 10000; i++) {
        testValues.push(rng());
      }
      const result = Bootstrap.estimate(testValues, Reducers.mean());
      // Strong constraints on the result.
      expect(result.lower).to.be.lessThan(result.value);
      expect(result.upper).to.be.greaterThan(result.value);
      // The actual mean should be pretty close to 0.5.
      expect(result.value).to.be.greaterThan(0.49);
      expect(result.value).to.be.lessThan(0.51);
      // We can add some soft constraints on the intervals too.
      expect(result.lower).to.be.greaterThan(0.48);
      expect(result.upper).to.be.lessThan(0.52);
    }
  });

  it("correct bounds across radixes", () => {
    // If the estimates span powers of 10, make sure this is handled correctly (js defaults to ordering numbers as strings).
    // This example produces such a situation.
    const result = Bootstrap.estimate(
      [1, 1, 2, 1, 2, 1, 2, 4, 6, 2, 7, 8, 23, 2, 3, 4, 5, 1, 3145, 3, 1, 24, 3],
      Reducers.mean()
    );
    expect(result.upper).to.be.greaterThan(result.lower);
  });
});
