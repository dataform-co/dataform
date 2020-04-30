import resolve from "@rollup/plugin-node-resolve";

// Add new node built ins here if they are used.
const knownBuiltIns = [
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
];

const importsToBundle = ["df", /df\/.*$/, /^bazel\-.*$/];

const checkImports = imports => {
  const allowedImports = [...imports, ...knownBuiltIns].map(pattern => {
    if (pattern instanceof RegExp) {
      return pattern;
    }
    const normalized = pattern.replace(/[\\^$*+?.()|[\]{}]/g, "\\$&");
    return new RegExp(`^${normalized}$`);
  });

  // We're going to read these from the arguments.
  let externals = [];

  return {
    buildStart(options) {
      externals = options.external || [];
    },
    resolveId(source) {
      // Either this is an internal import, or explicitly listed in externals or we fail.
      if (
        allowedImports.some(pattern => pattern.test(source)) ||
        externals.some(external => source === external || source.startsWith(`${external}/`))
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
