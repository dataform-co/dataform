import { expect } from "chai";

import { concatenateQueries } from "df/cli/api/dbadapters/tasks";
import { suite, test } from "df/testing";

suite("concatenateQueries", () => {
  test("adds semicolons to non-last statements and joins with newlines", () => {
    expect(concatenateQueries(["SELECT 1", "SELECT 2"])).deep.equals(
      "SELECT 1;\nSELECT 2"
    );
  });

  test("does not add semicolon to the last statement", () => {
    expect(concatenateQueries(["SELECT 1", "SELECT 2", "SELECT 3"])).deep.equals(
      "SELECT 1;\nSELECT 2;\nSELECT 3"
    );
  });

  test("preserves existing semicolons on non-last statements", () => {
    expect(concatenateQueries(["SELECT 1;", "SELECT 2"])).deep.equals(
      "SELECT 1;\nSELECT 2"
    );
  });

  test("preserves existing semicolons on last statement", () => {
    expect(concatenateQueries(["SELECT 1;", "SELECT 2;"])).deep.equals(
      "SELECT 1;\nSELECT 2;"
    );
  });

  test("adds semicolon before trailing comment if necessary, verifies fix for https://github.com/dataform-co/dataform/issues/1231", () => {
    expect(concatenateQueries(["SELECT 1 -- blah", "drop table foo; -- baz", "SELECT 2"])).deep.equals(
      "SELECT 1; -- blah\ndrop table foo; -- baz\nSELECT 2"
    );
  });

  test("preserves comment-only lines, verifies fix for https://github.com/dataform-co/dataform/issues/2103", () => {
    expect(concatenateQueries(["SELECT 1;", "--drop table foo", "SELECT 2", "--baz"])).deep.equals(
      "SELECT 1;\n--drop table foo\nSELECT 2;\n--baz"
    );
  });

  test("trims whitespace from statements", () => {
    expect(concatenateQueries(["  SELECT 1  ", "  SELECT 2  ", "  SELECT 3 -- blah"])).deep.equals(
      "SELECT 1;\nSELECT 2;\nSELECT 3 -- blah"
    );
  });

  test("filters out empty and falsy statements", () => {
    expect(concatenateQueries(["SELECT 1", "", "SELECT 2"])).deep.equals(
      "SELECT 1;\nSELECT 2"
    );
    expect(concatenateQueries(["SELECT 1", undefined, "SELECT 2"])).deep.equals(
      "SELECT 1;\nSELECT 2"
    );
  });

  test("returns empty string for empty input", () => {
    expect(concatenateQueries([])).deep.equals("");
  });

  test("returns single statement without semicolon", () => {
    expect(concatenateQueries(["SELECT 1"])).deep.equals("SELECT 1");
  });

  test("applies modifier to code portion of each statement", () => {
    expect(
      concatenateQueries(["SELECT 1", "SELECT 2"], stmt => `(${stmt})`)
    ).deep.equals("(SELECT 1);\n(SELECT 2)");
  });

  test("applies modifier only to code portion, not comment", () => {
    expect(
      concatenateQueries(["SELECT 1 --comment", "SELECT 2"], stmt => `(${stmt})`)
    ).deep.equals("(SELECT 1); --comment\n(SELECT 2)");
  });
});
