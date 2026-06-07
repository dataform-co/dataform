import { expect } from "chai";

import { parseArgv } from "df/common/flags";
import { suite, test } from "df/testing";

suite("flags", () => {
  suite("parseArgv", () => {
    test("parses a flag followed by its value", () => {
      expect(parseArgv(["node", "script", "--foo", "bar"])).deep.equals({ foo: "bar" });
    });

    test("parses a --flag=value pair", () => {
      expect(parseArgv(["node", "script", "--foo=bar"])).deep.equals({ foo: "bar" });
    });

    test("parses multiple flags", () => {
      expect(parseArgv(["node", "script", "--foo", "bar", "--baz", "qux"])).deep.equals({
        foo: "bar",
        baz: "qux"
      });
    });

    test("ignores positional arguments before flags", () => {
      expect(parseArgv(["node", "script", "run", "my_project", "--foo", "bar"])).deep.equals({
        foo: "bar"
      });
    });

    test("ignores positional arguments after a flag and its value", () => {
      // Regression test for https://github.com/dataform-co/dataform/issues/2198: a positional
      // argument after a flag used to throw "Arg neither flag name nor flag value", which
      // crashed the CLI.
      expect(parseArgv(["node", "script", "run", "--foo", "bar", "my_project"])).deep.equals({
        foo: "bar"
      });
    });

    test("ignores positional arguments interleaved with flags", () => {
      expect(parseArgv(["node", "script", "--foo", "bar", "pos", "--baz", "qux"])).deep.equals({
        foo: "bar",
        baz: "qux"
      });
    });

    test("returns an empty object when no flags are present", () => {
      expect(parseArgv(["node", "script", "run", "my_project"])).deep.equals({});
    });

    test("treats a --no- prefix as setting the flag to false", () => {
      expect(parseArgv(["node", "script", "--no-foo"])).deep.equals({ foo: "false" });
    });

    test("records a value-less trailing flag as an empty string", () => {
      expect(parseArgv(["node", "script", "--foo"])).deep.equals({ foo: "" });
    });
  });
});
