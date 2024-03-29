import { expect } from "chai";
import { basename } from "path";

import {
  JSONObjectStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
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

  suite("stringified map", () => {
    const jsonStringifiedMap = new StringifiedMap<IKey, string>(new JSONObjectStringifier(), [
      [{ a: "1", b: 2 }, "x"],
      [{ a: "2", b: 2 }, "y"],
      [{ a: "2", b: 3 }, "z"]
    ]);

    test("has correct stringified keys", () => {
      expect([...jsonStringifiedMap.keys()]).deep.equals([
        { a: "1", b: 2 },
        { a: "2", b: 2 },
        { a: "2", b: 3 }
      ]);
    });

    test("has correct stringified iterator", () => {
      expect([...jsonStringifiedMap]).deep.equals([
        [{ a: "1", b: 2 }, "x"],
        [{ a: "2", b: 2 }, "y"],
        [{ a: "2", b: 3 }, "z"]
      ]);
    });

    test("set and get", () => {
      const map = new StringifiedMap<IKey, string>(new JSONObjectStringifier());
      map.set(
        {
          a: "1",
          b: 2
        },
        "test"
      );
      expect(
        map.get({
          a: "1",
          b: 2
        })
      ).equals("test");
    });
  });

  suite("stringified set", () => {
    const jsonStringifiedSet = new StringifiedSet<IKey>(new JSONObjectStringifier(), [
      { a: "1", b: 2 },
      { a: "2", b: 2 },
      { a: "2", b: 3 }
    ]);

    test("has correct stringified values", () => {
      expect([...jsonStringifiedSet.values()]).deep.equals([
        { a: "1", b: 2 },
        { a: "2", b: 2 },
        { a: "2", b: 3 }
      ]);
    });

    test("has correct stringified iterator", () => {
      expect([...jsonStringifiedSet]).deep.equals([
        { a: "1", b: 2 },
        { a: "2", b: 2 },
        { a: "2", b: 3 }
      ]);
    });

    test("add and has", () => {
      const set = new StringifiedSet<IKey>(new JSONObjectStringifier());
      set.add({
        a: "1",
        b: 2
      });
      expect(
        set.has({
          a: "1",
          b: 2
        })
      ).equals(true);
    });
  });
});
