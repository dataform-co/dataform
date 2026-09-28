import { expect } from "chai";

import * as Path from "df/core/path";
import { suite, test } from "df/testing";

suite("path utils", () => {
  test("separator is always forward slash", () => {
    expect(Path.separator).equals("/");
  });

  test("toPosixPath converts Windows backslashes to forward slashes", () => {
    expect(Path.toPosixPath("definitions\\table.sqlx")).equals("definitions/table.sqlx");
    expect(Path.toPosixPath("C:\\Users\\runner\\project\\file.sqlx")).equals(
      "C:/Users/runner/project/file.sqlx",
    );
    expect(Path.toPosixPath("definitions/table.sqlx")).equals("definitions/table.sqlx");
    expect(Path.toPosixPath("")).equals("");
  });

  test("filename extracts file name with POSIX and Windows slashes", () => {
    expect(Path.filename("definitions/table.sqlx")).equals("table.sqlx");
    expect(Path.filename("definitions\\table.sqlx")).equals("table.sqlx");
    expect(Path.filename("C:\\Users\\runner\\project\\definitions\\table.sqlx")).equals(
      "table.sqlx",
    );
    expect(Path.filename("table.sqlx")).equals("table.sqlx");
  });

  test("basename extracts base name without extension", () => {
    expect(Path.basename("definitions/table.sqlx")).equals("table");
    expect(Path.basename("definitions\\table.sqlx")).equals("table");
    expect(Path.basename("C:\\Users\\runner\\project\\definitions\\table.sqlx")).equals("table");
    expect(Path.basename("no_ext")).equals("no_ext");
    expect(Path.basename("multi.dot.name.sqlx")).equals("multi.dot.name");
  });

  test("dirName extracts directory path with POSIX and Windows slashes", () => {
    expect(Path.dirName("definitions/table.sqlx")).equals("definitions");
    expect(Path.dirName("definitions\\table.sqlx")).equals("definitions");
    expect(Path.dirName("C:\\project\\definitions\\table.sqlx")).equals("C:/project/definitions");
    expect(Path.dirName("table.sqlx")).equals("");
  });

  test("relativePath normalizes to forward slashes across platforms", () => {
    expect(Path.relativePath("/path/to/project/definitions/file.sqlx", "/path/to/project")).equals(
      "definitions/file.sqlx",
    );
    expect(Path.relativePath("C:\\project\\definitions\\file.sqlx", "C:\\project")).equals(
      "definitions/file.sqlx",
    );
    expect(Path.relativePath("c:\\project\\definitions\\file.sqlx", "C:\\project")).equals(
      "definitions/file.sqlx",
    );
    expect(Path.relativePath("C:/project/definitions/file.sqlx", "C:\\project")).equals(
      "definitions/file.sqlx",
    );
    expect(Path.relativePath("file.sqlx", "")).equals("file.sqlx");
  });

  test("join combines paths with forward slashes", () => {
    expect(Path.join("a", "b", "c")).equals("a/b/c");
    expect(Path.join("a\\b", "c\\d")).equals("a/b/c/d");
    expect(Path.join("/a/", "/b/")).equals("a/b");
  });

  test("fileExtension extracts extension", () => {
    expect(Path.fileExtension("definitions/file.sqlx")).equals("sqlx");
    expect(Path.fileExtension("definitions\\file.sqlx")).equals("sqlx");
    expect(Path.fileExtension("no_extension")).equals("");
  });

  test("normalize handles both slash types and dot navigation", () => {
    expect(Path.normalize("a/b/../c")).equals("a/c");
    expect(Path.normalize("a\\b\\..\\c")).equals("a/c");
    expect(Path.normalize("/a/b/../../c")).equals("/c");
  });
});
