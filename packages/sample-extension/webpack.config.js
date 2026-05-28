const path = require("path");
const webpack = require("webpack");

const fs = require("fs");

module.exports = (env, argv) => {
  const runfilesDir = process.env.RUNFILES;
  let workspaceName = "df";
  if (!fs.existsSync(path.resolve(runfilesDir, "df"))) {
    workspaceName = "_main";
  }

  const config = {
    mode: argv.mode || "development",
    target: 'node',
    entry: [path.resolve(runfilesDir, workspaceName, "packages/sample-extension/index")],
    output: {
      libraryTarget: "commonjs-module",
    },
    optimization: {
      minimize: true
    },
    stats: {
      warnings: true
    },
    resolve: {
      extensions: [".ts", ".js", ".json"],
      alias: {
        df: path.resolve(runfilesDir, workspaceName)
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
