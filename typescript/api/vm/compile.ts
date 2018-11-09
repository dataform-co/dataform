import * as path from "path";
import { NodeVM } from "vm2";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import { genIndex } from "../gen_index";

export function compile(projectDir: string): protos.ICompiledGraph {
  const vm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: projectDir,
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: (code, file) => {
      if (file.endsWith(".assert.sql")) {
        return utils.compileAssertionSql(code, file);
      }
      if (file.endsWith(".ops.sql")) {
        return utils.compileOperationSql(code, file);
      }
      if (file.endsWith(".sql")) {
        return utils.compileMaterializationSql(code, file);
      }
      return code;
    }
  });
  var indexScript = genIndex(projectDir);
  return vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
}

process.on(`message`, object => {
  try {
    var graph = compile(object.projectDir);
    process.send({ result: graph });
  }
  catch (e) {
    process.send({ err: e });
  }
  process.exit();
})
