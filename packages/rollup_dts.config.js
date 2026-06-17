import dts from "rollup-plugin-dts";
import * as path from "path";
import * as fs from "fs";

function findBazelBin() {
  let dir = process.cwd();
  const bazelOutDir = path.join(dir, "bazel-out");
  if (fs.existsSync(bazelOutDir) && fs.statSync(bazelOutDir).isDirectory()) {
    const configs = fs.readdirSync(bazelOutDir);
    for (const config of configs) {
      const binPath = path.join(bazelOutDir, config, "bin");
      if (fs.existsSync(binPath) && fs.statSync(binPath).isDirectory()) {
        return binPath;
      }
    }
  }
  return dir;
}

const bazelBin = findBazelBin();

function resolveDtsPlugin() {
  return {
    name: "resolve-dts",
    resolveId(source) {
      if (source.startsWith("df/") || source.startsWith("packages/")) {
        const relPath = source.startsWith("df/") ? source.slice(3) : source;
        const candidates = [
          path.resolve(bazelBin, relPath),
          path.resolve(process.cwd(), relPath),
        ];
        
        for (const candidate of candidates) {
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
