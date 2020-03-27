import { IHookHandler } from "@dataform/testing";
import * as fs from "fs";
import * as path from "path";
import * as rimraf from "rimraf";

// TmpDirFixture can be used to create unique temporary directories which will be cleaned up
// at the end of a test run. Intended for use within bazel tests.
export class TmpDirFixture {
  private static dirCounter = 0;

  private tmpDirPaths: Set<string>;

  constructor(tearDown: IHookHandler) {
    this.tmpDirPaths = new Set<string>();
    tearDown("delete tmp directories", () => this.rmTmpDirs());
  }

  public createNewTmpDir() {
    // TEST_TMPDIR is set by bazel.
    const tmpDirPath = path.resolve(
      path.join(process.env.TEST_TMPDIR, `tmp_dir_${TmpDirFixture.dirCounter++}`)
    );
    // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
    fs.mkdirSync(tmpDirPath);
    this.tmpDirPaths.add(tmpDirPath);
    return tmpDirPath;
  }

  private rmTmpDirs() {
    for (const tmpPath of this.tmpDirPaths) {
      rimraf.sync(tmpPath);
    }
  }
}
