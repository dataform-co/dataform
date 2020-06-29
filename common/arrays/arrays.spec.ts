import { expect } from "chai";
import { basename } from "path";

import { flatten } from "df/common/arrays/arrays";
import { suite, test } from "df/testing";

suite(basename(__filename), () => {
  test("can flatten nested arrays", () => {
    expect(
      flatten([
        [1, 2],
        [3, 4]
      ])
    ).deep.equals([1, 2, 3, 4]);
  });
});
