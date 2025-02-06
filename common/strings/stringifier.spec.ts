import { expect } from "chai";
import { basename } from "path";

import { JSONObjectStringifier } from "df/common/strings/stringifier";
import { suite, test } from "df/testing";

interface IKey {
  a: string;
  b: number;
}

suite(basename(__filename), () => {
  suite("json object stringifier", () => {
    test("serialize and deserialize", () => {
      const stringifier = new JSONObjectStringifier();
      const value = {
        a: "test",
        b: 123,
        c: true,
        d: {
          v: "inner"
        },
        e: null as null
      };
      expect(stringifier.parse(stringifier.stringify(value))).deep.equals(value);
    });

    test("is insensitive to key order", () => {
      const stringifier = new JSONObjectStringifier();
      const value = {
        a: "test",
        b: 123,
        c: true,
        d: {
          v: "inner"
        }
      };
      const value2 = {
        d: {
          v: "inner"
        },
        a: "test",
        c: true,
        b: 123
      };
      expect(stringifier.stringify(value)).equals(stringifier.stringify(value2));
    });
  });
});
