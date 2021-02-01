const path = require("path");
const webpack = require("webpack");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    entry: [path.resolve(process.env.RUNFILES, "df/sandbox/vm/worker")],
    output: {
      path: path.dirname(path.resolve(argv.outputPath)),
      filename: path.basename(argv.outputPath)
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
    node: {
      __dirname: true
    },
    resolve: {
      extensions: [".ts", ".js"],
      alias: {
        df: path.resolve(process.env.RUNFILES, "df")
      }
    },
    plugins: [
      new webpack.optimize.LimitChunkCountPlugin({
        maxChunks: 1
      })
    ],
    externals: {
      // vm2: "require('vm2')"
    }
  };
  return config;
};
