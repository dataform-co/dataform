import resolve from "@rollup/plugin-node-resolve";
import * as path from "path";
import * as fs from "fs";

function convertToRegex(pattern) {
  if (pattern instanceof RegExp) {
    return pattern;
  }
  const normalized = pattern.replace(/[\\^$*+?.()|[\]{}]/g, "\\$&");
  return new RegExp(`^${normalized}$`);
}

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
  "net"
].map(moduleName => convertToRegex(moduleName));

const importsToBundle = ["df", /df\/.*$/, /^bazel\-.*$/];

const checkImports = imports => {
  const allowedImports = [...imports].map(pattern => convertToRegex(pattern));
  let externals = () => false;
  let allowNodeBuiltins = process.env.ALLOW_NODE_BUILTINS;

  return {
    buildStart(options) {
      externals = options.external || (() => false);
    },
    resolveId(source) {
      if (source.startsWith("\u0000") || source.includes("\u0000")) {
        return undefined;
      }

      if (path.isAbsolute(source) || source.startsWith("/")) {
        return undefined;
      }

      if (source.startsWith("df/") || source.startsWith("packages/")) {
        const relPath = source.startsWith("df/") ? source.slice(3) : source;

        // Compute three possible candidates:
        const candidates = [
          path.resolve(process.cwd(), relPath),
          path.resolve(process.cwd(), "../../..", "bazel-out", "k8-fastbuild-py2", "bin", relPath),
          path.resolve(process.cwd(), "bazel-out", "k8-fastbuild-py2", "bin", relPath)
        ];

        const esmCandidates = [];
        const normalCandidates = [];
        for (const candidate of candidates) {
          normalCandidates.push(candidate);
          // Generate ESM variants by walking up the directory tree
          let dir = candidate;
          let suffix = "";
          while (dir && dir !== "/" && dir !== ".") {
            const esmDir = path.join(dir, "esm");
            if (fs.existsSync(esmDir) && fs.statSync(esmDir).isDirectory()) {
              const esmPath = suffix ? path.join(esmDir, suffix) : esmDir;
              esmCandidates.push(esmPath);
            }

            const base = path.basename(dir);
            if (base === "bin" || base === "migration" || base === "dataform_migrated") {
              break;
            }
            suffix = suffix ? path.join(base, suffix) : base;
            dir = path.dirname(dir);
          }
        }

        const allCandidates = [...esmCandidates, ...normalCandidates];

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

      if (source.startsWith(".") || source.startsWith("bazel-out/")) {
        return undefined;
      }

      if (
        allowedImports.some(pattern => pattern.test(source)) ||
        externals(source) ||
        externals(source.split("/")[0]) ||
        (allowNodeBuiltins && knownNodeBuiltins.some(pattern => pattern.test(source)))
      ) {
        return null;
      }
      throw new Error("Must explicitly list import as an external: " + source);
    }
  };
};

export default {
  external: id => {
    if (id.startsWith("df/") || id === "df") return false;
    if (id.startsWith("bazel-")) return false;
    if (id.startsWith(".") || id.startsWith("/") || require("path").isAbsolute(id)) return false;
    if (id.includes("commonjsHelpers")) return false;
    return true;
  },
  plugins: [
    checkImports(importsToBundle),
    resolve({
      resolveOnly: importsToBundle
    })
  ],
};
