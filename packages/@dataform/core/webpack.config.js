const path = require("path");
const webpack = require("webpack");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    target: 'node',
    entry: [path.resolve(process.env.RUNFILES, "df/packages/@dataform/core/index")],
    output: {
      path: path.resolve(argv.outputPath),
      filename: path.basename(argv.outputFilename),
      libraryTarget: "commonjs",
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
