import { expect } from "chai";
import * as path from "path";

import { isRealPathContained } from "df/common/vm/path_utils";
import { suite, test } from "df/testing";

suite("path_utils", () => {
  suite("isRealPathContained", () => {
    test("accepts paths inside parentDir including dot-prefixed filenames", () => {
      const parentDir = path.resolve("/workspace/project");

      expect(isRealPathContained(parentDir, parentDir)).to.equal(true);
      expect(
        isRealPathContained(path.join(parentDir, "definitions", "table.sqlx"), parentDir),
      ).to.equal(true);
      expect(isRealPathContained(path.join(parentDir, "..dotprefixed.sqlx"), parentDir)).to.equal(
        true,
      );
      expect(
        isRealPathContained(path.join(parentDir, "definitions", "..nested_dot.sqlx"), parentDir),
      ).to.equal(true);
    });

    test("rejects parent, sibling and prefix-colliding directories", () => {
      const parentDir = path.resolve("/workspace/project");

      expect(isRealPathContained(path.resolve("/workspace"), parentDir)).to.equal(false);
      expect(isRealPathContained(path.resolve("/workspace/other/file.sqlx"), parentDir)).to.equal(
        false,
      );
      expect(
        isRealPathContained(path.resolve("/workspace/project-other/file.sqlx"), parentDir),
      ).to.equal(false);
    });
  });
});
