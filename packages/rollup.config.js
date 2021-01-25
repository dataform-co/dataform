import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import { babel } from "@rollup/plugin-babel";

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

const importsToBundle = [
  "df",
  /df\/.*$/,
  /^bazel\-.*$/,
  /\.\/.*$/,
  /\.\.\/.*$/,
  "vm2",
  "glob",
  "protobufjs",
  "protobufjs/minimal",
  "@protobufjs/codegen",
  "@protobufjs/fetch",
  "@protobufjs/path",
  "@protobufjs/aspromise",
  "@protobufjs/inquire"
];

const checkImports = imports => {
  const allowedImports = [...imports, ...knownBuiltIns].map(pattern => {
    if (pattern instanceof RegExp) {
      return pattern;
    }
    // If it's a string, turn it into a regex, by escaping any regex characters in the string.
    const normalized = pattern.replace(/[\\^$*+?.()|[\]{}]/g, "\\$&");
    return new RegExp(`^${normalized}$`);
  });

  // We're going to read these from the arguments.
  let external = () => false;

  return {
    buildStart(options) {
      external = options.external;
    },
    resolveId(source) {
      // Either this is an internal import, or explicitly listed in externals or we fail.
      // console.log(external.toString());
      if (allowedImports.some(pattern => pattern.test(source)) || external(source)) {
        return null;
      }
      throw new Error("Must explicitly list import as an external: " + source);
    }
  };
};

// CommonJS module should go before babel:
// https://github.com/rollup/rollup-plugin-babel/issues/342
// HOWEVER:
// Discussion of difficulty of running CommonJs then Babel (as opposed running Babel then CommonJS):
// https://github.com/rollup/rollup-plugin-commonjs/issues/29
// This is something explicitly supported by Webpack, "Bundles ES Modules, CommonJS, and AMD modules (even combined)."
// https://github.com/webpack/webpack

export default {
  plugins: [
    json(),
    resolve({
      jsnext: true,
      browser: false
      // resolveOnly: importsToBundle,
    }),
    commonjs(),
    babel({ babelHelpers: "bundled", babelrc: false, presets: ["@babel/preset-env"] })
    // checkImports(importsToBundle),
  ]
};
