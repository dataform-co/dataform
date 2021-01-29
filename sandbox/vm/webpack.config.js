const path = require("path");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    entry: [path.resolve(process.env.RUNFILES, "df/sandbox/vm/worker")],
    output: {
      path: path.dirname(path.resolve("sandbox/vm/worker_bundle.js")),
      filename: path.basename("sandbox/vm/worker_bundle.js")
    },
    target: "node",
    optimization: {
      minimize: argv.mode === "production"
    },
    stats: {
      warnings: true
    },
    // node: {
    //   fs: "empty",
    //   child_process: "empty"
    // },
    resolve: {
      extensions: [".ts", ".js"],
      alias: {
        df: path.resolve(process.env.RUNFILES, "df")
      }
    },
    externals: {
      // vm2: "node-modules/vm2"
    }
  };
  return config;
};
