import resolve from "@rollup/plugin-node-resolve";
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

function convertToRegex(pattern) {
  if (pattern instanceof RegExp) {
    return pattern;
  }
  // If it's a string, turn it into a regex, by escaping any regex characters in the string.
  const normalized = pattern.replace(/[\\^$*+?.()|[\]{}]/g, "\\$&");
  return new RegExp(`^${normalized}$`);
}

// Add new node built ins here if they are used.
const knownNodeBuiltins = [
  "path",
  "fs",
  "os",
  "util",
  "child_process",
  "crypto",
  "events",
  "long",
  "https",
  "net",
].map((moduleName) => convertToRegex(moduleName));

const importsToBundle = ["df", /df\/.*$/, /^bazel\-.*$/];

const checkImports = (imports) => {
  const allowedImports = [...imports].map((pattern) => convertToRegex(pattern));
  // We're going to read these from the arguments.
  let externals = () => false;
  let allowNodeBuiltins = process.env.ALLOW_NODE_BUILTINS;

  return {
    buildStart(options) {
      externals = options.external || (() => false);
    },
    resolveId(source) {
      if (path.isAbsolute(source) || source.startsWith(".")) {
        return undefined;
      }

      if (source.startsWith("df/") || source.startsWith("packages/")) {
        const relPath = source.startsWith("df/") ? source.slice(3) : source;

        const bazelBin = findBazelBin();
        const candidate = bazelBin
          ? path.resolve(bazelBin, relPath)
          : path.resolve(process.cwd(), relPath);

        const esmCandidates = [];
        // Generate ESM variants by walking up the directory tree
        let dir = candidate;
        let suffix = "";
        while (dir && dir !== "/" && dir !== ".") {
          const esmDir = path.join(dir, "esm");
          if (fs.existsSync(esmDir) && fs.statSync(esmDir).isDirectory()) {
            const esmPath = suffix ? path.join(esmDir, suffix) : esmDir;
            esmCandidates.push(esmPath);
          }

          const parent = path.dirname(dir);
          if (parent === dir) {
            break;
          }
          const base = path.basename(dir);
          if (base === "bin") {
            break;
          }
          suffix = suffix ? path.join(base, suffix) : base;
          dir = parent;
        }

        const allCandidates = [...esmCandidates, candidate];

        for (const candidate of allCandidates) {
          if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
            return candidate;
          }
          if (fs.existsSync(candidate + ".js") && fs.statSync(candidate + ".js").isFile()) {
            return candidate + ".js";
          }
          const indexCandidate = path.resolve(candidate, "index.js");
          if (fs.existsSync(indexCandidate) && fs.statSync(indexCandidate).isFile()) {
            return indexCandidate;
          }
        }
      }

      if (allowedImports.some((pattern) => pattern.test(source))) {
        return null;
      }
      if (
        externals(source) ||
        externals(source.split("/")[0]) ||
        (allowNodeBuiltins && knownNodeBuiltins.some((pattern) => pattern.test(source)))
      ) {
        return false;
      }
      throw new Error("Must explicitly list import as an external: " + source);
    },
  };
};

export default {
  plugins: [
    checkImports(importsToBundle),
    resolve({
      resolveOnly: importsToBundle,
    }),
  ],
};
