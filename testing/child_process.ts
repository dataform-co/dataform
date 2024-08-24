import * as fs from "fs";
import * as path from "path";

import { ChildProcess, spawn } from "child_process";
import { IHookHandler } from "#df/testing";

export class ChildProcessForBazelTestEnvironment {
  private childProcess: ChildProcess;

  constructor(private readonly executable: string, private readonly args: string[] = []) {}

  public spawn(options: { pipeOutputToParentOutputs?: boolean; cwd?: string } = {}) {
    this.childProcess = spawn(this.executable, this.args, options);
    this.childProcess.stdout.pipe(this.createFileWriteStream(".STDOUT.txt"));
    this.childProcess.stderr.pipe(this.createFileWriteStream(".STDERR.txt"));
    if (options.pipeOutputToParentOutputs) {
      this.childProcess.stdout.pipe(process.stdout);
      this.childProcess.stderr.pipe(process.stderr);
    }
  }

  public kill() {
    this.childProcess.kill();
  }

  public asFixture(setUp: IHookHandler, tearDown: IHookHandler): this {
    setUp(`spawning ${this.executable}`, () => this.spawn());
    tearDown(`killing ${this.executable}`, () => this.kill());
    return this;
  }

  private createFileWriteStream(fileExtension: string) {
    // tslint:disable: tsr-detect-non-literal-fs-filename
    return fs.createWriteStream(
      path.resolve(
        process.env.TEST_UNDECLARED_OUTPUTS_DIR,
        this.executable.replace(/\//g, "_") + fileExtension
      )
    );
  }
}
