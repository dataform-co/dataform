import * as protos from "@dataform/protos";

import { fork } from "child_process";

export function compile(projectDir: string): Promise<protos.ICompiledGraph> {
  console.log("DEBUG: pre_fork:\t " + Date.now());
  var child = fork(require.resolve("../vm/compile"));
  console.log("DEBUG: post_fork:\t " + Date.now());
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
      console.log("DEBUG: on_message:\t " + Date.now());
      if (!child.killed) child.kill();
      if (obj.err) {
        reject(new Error(obj.err));
      } else {
        console.log("DEBUG: pre_resolve:\t " + Date.now());
        resolve(protos.CompiledGraph.create(obj.result));
      }
    });
    console.log("DEBUG: pre_send:\t " + Date.now());
    child.send({ projectDir });
    console.log("DEBUG: post_send:\t " + Date.now());
  });
}
