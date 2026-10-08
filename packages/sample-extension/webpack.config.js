const webpack = require("webpack");

module.exports = (env, argv) => {
  if (!process.env.BAZEL_BINDIR) {
    throw new Error("BAZEL_BINDIR must be set");
  }

  const config = {
    mode: argv.mode || "development",
    target: "node",
    devtool: false,
    output: {
      libraryTarget: "commonjs-module",
    },
    optimization: {
      minimize: true,
    },
    stats: {
      warnings: true,
    },
    resolve: {
      extensions: [".js", ".json"],
      alias: {
        df: process.cwd(),
      },
    },
    plugins: [
      new webpack.optimize.LimitChunkCountPlugin({
        maxChunks: 1,
      }),
    ],
  };
  return config;
};
