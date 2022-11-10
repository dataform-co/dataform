import resolve from "@rollup/plugin-node-resolve";

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
  "net"
].map(moduleName => convertToRegex(moduleName));

const importsToBundle = ["df", /df\/.*$/, /^bazel\-.*$/];

const checkImports = imports => {
  const allowedImports = [...imports].map(pattern => convertToRegex(pattern));

  // We're going to read these from the arguments.
  let externals = [];
  let allowNodeBuiltins = process.env.ALLOW_NODE_BUILTINS;

  return {
    buildStart(options) {
      externals = options.external || [];
    },
    resolveId(source) {
      // Either this is an internal import, or explicitly listed in externals or we fail.
      if (
        allowedImports.some(pattern => pattern.test(source)) ||
        externals.some(external => source === external || source.startsWith(`${external}/`)) ||
        (allowNodeBuiltins && knownNodeBuiltins.some(pattern => pattern.test(source)))
      ) {
        return null;
      }
      throw new Error("Must explicitly list import as an external: " + source);
    }
  };
};

export default {
  plugins: [
    checkImports(importsToBundle),
    resolve({
      resolveOnly: importsToBundle
    })
  ]
};
