import { expect } from "chai";
import {
  ArrayStringifier,
  JSONObjectStringifier,
  LongStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
import { suite, test } from "df/testing";
import Long from "long";
import { basename } from "path";

interface IKey {
  a: string;
  b: number;
}

suite(basename(__filename), () => {
  suite("json object stringifier", () => {
    test("serialize and deserialize", () => {
      const stringifier = JSONObjectStringifier.create();
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
      const stringifier = JSONObjectStringifier.create();
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

  suite("array stringifier", () => {
    test("serialize and deserialize", () => {
      const stringifier = ArrayStringifier.create(LongStringifier.create());
      const value = [Long.fromNumber(12345), Long.fromNumber(12345234)];
      expect(stringifier.parse(stringifier.stringify(value))).deep.equals(value);
    });
  });

  suite("stringified map", () => {
    const jsonStringifiedMap = new StringifiedMap<IKey, string>(JSONObjectStringifier.create(), [
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
      const map = new StringifiedMap<IKey, string>(JSONObjectStringifier.create());
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
    const jsonStringifiedSet = new StringifiedSet<IKey>(JSONObjectStringifier.create(), [
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
      const set = new StringifiedSet<IKey>(JSONObjectStringifier.create());
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
