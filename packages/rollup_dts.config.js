import * as fs from "fs";
import * as path from "path";
import dts from "rollup-plugin-dts";

const tsconfig = JSON.parse(fs.readFileSync("tsconfig.json", "utf8"));

export default {
  plugins: [
    dts({
      respectExternal: true,
      compilerOptions: {
        baseUrl: path.resolve(tsconfig.compilerOptions.baseUrl),
        paths: tsconfig.compilerOptions.paths,
      },
    }),
  ],
};
