const path = require("path");
const webpack = require("webpack");

const fs = require("fs");

module.exports = (env, argv) => {
  const binDir = process.cwd().endsWith("bin")
    ? process.cwd()
    : path.resolve(process.cwd(), process.env.BAZEL_BINDIR || ".");

  const config = {
    mode: argv.mode || "production",
    target: "node",
    devtool: false,
    output: {
      libraryTarget: "commonjs-module"
    },
    optimization: {
      minimize: true,
      moduleIds: "deterministic"
    },
    stats: {
      warnings: true
    },
    resolve: {
      extensions: [".js", ".ts", ".json"],
      alias: {
        df: binDir
      }
    },
    plugins: [
      new webpack.optimize.LimitChunkCountPlugin({
        maxChunks: 1
      })
    ]
  };
  return config;
};
