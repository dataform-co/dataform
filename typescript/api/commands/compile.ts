import * as protos from "@dataform/protos";

import { fork } from "child_process";

export function compile(projectDir: string): Promise<protos.ICompiledGraph> {
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
    }
    checkTimeout();
    child.on("message", obj => {
      if (!child.killed) child.kill();
      if (obj.err) {
        reject(obj.err);
      } else {
        resolve(protos.CompiledGraph.create(obj.result));
      }
    });
    child.send({ projectDir });
  });
}
