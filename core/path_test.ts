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

  test("toPosixPath keeps the UNC prefix of Windows network paths", () => {
    expect(Path.toPosixPath("\\\\server\\share\\proj")).equals("//server/share/proj");
    expect(Path.toPosixPath("\\\\server\\share\\proj\\definitions\\a.sqlx")).equals(
      "//server/share/proj/definitions/a.sqlx",
    );
  });

  test("comparablePath lower-cases only the Windows drive letter", () => {
    expect(Path.comparablePath("C:\\Proj\\Defs")).equals("c:/Proj/Defs");
    expect(Path.comparablePath("c:/Proj")).equals("c:/Proj");
    expect(Path.comparablePath("C:")).equals("c:");
    expect(Path.comparablePath("/home/Alice")).equals("/home/Alice");
    expect(Path.comparablePath("Definitions/A.sqlx")).equals("Definitions/A.sqlx");
    // Not a drive letter: no slash (or end of string) after the colon.
    expect(Path.comparablePath("C:Proj")).equals("C:Proj");
  });

  test("startsWithPath is case-sensitive except for Windows drive letters", () => {
    expect(Path.startsWithPath("/tmp/Proj/definitions/a.sqlx", "/tmp/Proj")).equals(true);
    expect(Path.startsWithPath("/tmp/Proj", "/tmp/Proj")).equals(true);
    expect(Path.startsWithPath("/tmp/Proj/", "/tmp/Proj")).equals(true);
    expect(Path.startsWithPath("/tmp/Proj/a.sqlx", "/tmp/Proj/")).equals(true);
    expect(Path.startsWithPath("/tmp/proj/secret.json", "/tmp/Proj")).equals(false);
    expect(Path.startsWithPath("/tmp/Projects/a.sqlx", "/tmp/Proj")).equals(false);
    expect(Path.startsWithPath("/other/a.sqlx", "/tmp/Proj")).equals(false);
    expect(Path.startsWithPath("c:\\proj\\a.sqlx", "C:/proj")).equals(true);
    expect(Path.startsWithPath("C:/proj/a.sqlx", "c:\\proj")).equals(true);
    expect(Path.startsWithPath("C:/Proj/a.sqlx", "C:/proj")).equals(false);
    expect(Path.startsWithPath("D:/proj/a.sqlx", "C:/proj")).equals(false);
    expect(
      Path.startsWithPath("\\\\server\\share\\proj\\a.sqlx", "\\\\server\\share\\proj"),
    ).equals(true);
    expect(Path.startsWithPath("/server/share/proj/a.sqlx", "\\\\server\\share\\proj")).equals(
      false,
    );
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
    expect(Path.relativePath("/path/to/project/definitions/file.sqlx", "/path/to/project/")).equals(
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

  test("relativePath ignores case only in the Windows drive letter", () => {
    expect(Path.relativePath("/home/Alice/definitions/a.sqlx", "/home/alice")).equals(
      "/home/Alice/definitions/a.sqlx",
    );
    expect(Path.relativePath("C:/Project/definitions/a.sqlx", "C:/project")).equals(
      "C:/Project/definitions/a.sqlx",
    );
  });

  test("relativePath returns empty string for the base itself and the full path when outside", () => {
    expect(Path.relativePath("/home/alice", "/home/alice")).equals("");
    expect(Path.relativePath("/home/alice/", "/home/alice")).equals("");
    expect(Path.relativePath("C:\\project", "c:/project")).equals("");
    expect(Path.relativePath("/other/z.sqlx", "/home/alice")).equals("/other/z.sqlx");
    expect(Path.relativePath("/home/alice-other/z.sqlx", "/home/alice")).equals(
      "/home/alice-other/z.sqlx",
    );
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
