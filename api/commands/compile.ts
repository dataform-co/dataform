import * as protos from "@dataform/protos";

import { fork } from "child_process";
import * as fs from "fs";
import { promisify } from "util";
import * as path from "path";

export function compile(projectDir: string): Promise<protos.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  projectDir = path.resolve(projectDir);
  var child = fork(require.resolve("../vm/compile"));
  return new Promise((resolve, reject) => {
    var timeout = 5000;
    var timeoutStart = Date.now();
    var checkTimeout = () => {
      if (child.killed) return;
      if (Date.now() > timeoutStart + timeout) {
        child.kill();
        reject(new Error("Compilation timed out"));
      } else {
        setTimeout(checkTimeout, 100);
      }
    };
    checkTimeout();
    child.on("message", obj => {
      if (!child.killed) child.kill();
      if (obj.err) {
        reject(new Error(obj.err));
      } else {
        // We receive back a path where the compiled graph was written in proto format.
        promisify(fs.readFile)(obj.path)
          .then(contents => protos.CompiledGraph.decode(contents))
          .then(graph => resolve(graph));
      }
    });
    child.send({ projectDir });
  });
}
