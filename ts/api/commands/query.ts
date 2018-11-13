import * as protos from "@dataform/protos";
import * as dbadapters from "../dbadapters";

import { fork } from "child_process";

export function run(profile: protos.IProfile, query: string, projectDir?: string): Promise<any[]> {
  return compile(query, projectDir).then(compiledQuery => dbadapters.create(profile).execute(compiledQuery));
}

export function compile(query: string, projectDir?: string): Promise<string> {
  var child = fork(require.resolve("../vm/query"));
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
        reject(new Error(obj.err));
      } else {
        resolve(obj.result);
      }
    });
    child.send({ query, projectDir });
  });
}
