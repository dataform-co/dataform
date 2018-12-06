import * as path from "path";
import { NodeVM } from "vm2";
import { compilers } from "@dataform/core";
import { genIndex } from "../gen_index";

export function compile(query: string, projectDir?: string): string {
  var compiledQuery = query;
  if (projectDir) {
    const vm = new NodeVM({
      wrapper: "none",
      require: {
        context: "sandbox",
        root: projectDir,
        external: true
      },
      sourceExtensions: ["js", "sql"],
      compiler: (code, path) => compilers.compile(code, path)
    });
    var indexScript = genIndex(
      projectDir,
      `(function() {
        const { session } = require("@dataform/core");
        const ref = session.ref.bind(session);
        const noop = () => "";
        const config = noop;
        const type = noop;
        const postOps = noop;
        const preOps = noop;
        const self = noop;
        const dependencies = noop;
        const where = noop;
        const descriptor = noop;
        const describe = field => field;
        return \`${query}\`;
      })()`
    );
    compiledQuery = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  }
  return compiledQuery;
}

process.on(`message`, object => {
  try {
    var graph = compile(object.query, object.projectDir);
    process.send({ result: graph });
  } catch (e) {
    process.send({ err: String(e) });
  }
  process.exit();
});
