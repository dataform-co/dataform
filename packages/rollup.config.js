import resolve from "@rollup/plugin-node-resolve";
import * as fs from "fs";
import * as path from "path";

const tsconfig = JSON.parse(fs.readFileSync("tsconfig.json", "utf8"));
const baseUrl = tsconfig.compilerOptions.baseUrl || ".";

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
    resolveId(source, importer) {
      if (!importer || path.isAbsolute(source) || source.startsWith(".")) {
        return null;
      }
      if (source.startsWith("df/")) {
        return this.resolve(path.resolve(baseUrl, source.slice(3)), importer, {
          skipSelf: true,
        });
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
    resolve(),
  ],
};
