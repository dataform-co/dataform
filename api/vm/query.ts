import { compilers, genIndex } from "@dataform/core";
import * as path from "path";
import { NodeVM } from "vm2";
import { getGenIndexConfig } from "./gen_index_config";

export function compile(query: string, projectDir?: string): string {
  let compiledQuery = query;
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
    const indexScript = genIndex(
      getGenIndexConfig(
        { projectDir },
        `(function() {
        const { session } = require("@dataform/core");
        const ref = session.resolve.bind(session);
        const resolve = session.resolve.bind(session);
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
      )
    );
    compiledQuery = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  }
  return compiledQuery;
}

process.on(`message`, object => {
  try {
    const graph = compile(object.query, object.projectDir);
    process.send({ result: graph });
  } catch (e) {
    process.send({ err: String(e) });
  }
  process.exit();
});
