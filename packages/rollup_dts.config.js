import dts from "rollup-plugin-dts";
import * as path from "path";
import * as fs from "fs";

function findBazelBin() {
  if (!process.env.BAZEL_BINDIR) {
    return undefined;
  }
  let dir = process.cwd();
  while (dir && !fs.existsSync(path.join(dir, "bazel-out"))) {
    const parent = path.dirname(dir);
    if (parent === dir) {
      break;
    }
    dir = parent;
  }
  return path.resolve(dir, process.env.BAZEL_BINDIR);
}

function resolveDtsPlugin() {
  return {
    name: "resolve-dts",
    resolveId(source) {
      if (source.startsWith("df/") || source.startsWith("packages/")) {
        const relPath = source.startsWith("df/") ? source.slice(3) : source;
        const bazelBin = findBazelBin();
        const candidate = bazelBin
          ? path.resolve(bazelBin, relPath)
          : path.resolve(process.cwd(), relPath);

        if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
          return candidate;
        }
        if (fs.existsSync(candidate + ".d.ts") && fs.statSync(candidate + ".d.ts").isFile()) {
          return candidate + ".d.ts";
        }
        const indexCandidate = path.resolve(candidate, "index.d.ts");
        if (fs.existsSync(indexCandidate) && fs.statSync(indexCandidate).isFile()) {
          return indexCandidate;
        }
      }
      return null;
    }
  };
}

export default {
  plugins: [
    resolveDtsPlugin(),
    dts({
      respectExternal: true
    })
  ]
};
