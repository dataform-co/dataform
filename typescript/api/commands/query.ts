import * as fs from "fs";
import * as util from "util";
import * as path from "path";
import { NodeVM } from "vm2";
import * as glob from "glob";
import { utils } from "@dataform/core";
import * as protos from "@dataform/protos";
import * as runners from "../runners";
import { genIndex } from "./compile";

export function run(profile: protos.IProfile, query: string, projectDir?: string): Promise<any[]> {
  var compiledQuery = compile(query, projectDir);
  return runners.create(profile).execute(compiledQuery);
}

export function compile(query: string, projectDir?: string) {
  var compiledQuery = query;
  if (projectDir) {
    const vm = new NodeVM({
      timeout: 5000,
      wrapper: "none",
      require: {
        context: "sandbox",
        root: projectDir,
        external: true
      },
      sourceExtensions: ["js", "sql"],
      compiler: (code, file) => {
        if (file.endsWith(".test.sql")) {
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
    var indexScript = genIndex(projectDir, `(function() {
        const ref = dataformcore.singleton.ref.bind(dataformcore.singleton);
        return \`${query}\`;
      })()`);
    compiledQuery = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  }
  return compiledQuery;
}
