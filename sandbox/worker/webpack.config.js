const path = require("path");
const webpack = require("webpack");
const fs = require("fs");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    target: 'node',
    entry: [path.resolve(process.env.RUNFILES, "df/sandbox/worker/worker")],
    output: {
      path: path.dirname(path.resolve(argv.output)),
      filename: path.basename(argv.output)
    },
    externals: {
      "vm2": "require('vm2')"
    },
    optimization: {
      minimize: true
    },
    stats: {
      warnings: true
    },
    node: {
      fs: "empty",
      child_process: "empty"
    },
    resolve: {
      extensions: [".ts", ".tsx", ".js", ".jsx", ".json", ".css"],
      alias: {
        df: path.resolve(process.env.RUNFILES, "df")
      }
    },

    plugins: [
      new webpack.optimize.LimitChunkCountPlugin({
        maxChunks: 1
      })
    ],

  };
  return config;
};
