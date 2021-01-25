const path = require("path");
const webpack = require("webpack");

module.exports = (env, argv) => {
  const config = {
    mode: "production",
    entry: [path.resolve(process.env.RUNFILES, "df/sandbox/vm/worker")],
    output: {
      path: path.dirname(path.resolve(argv.output)),
      filename: path.basename(argv.output)
    },
    optimization: {
      minimize: false // TODO: true?
    },
    // node: {
    //   fs: "empty",
    //   child_process: "empty"
    // },
    resolve: {
      extensions: [".ts", ".js", ".json"]
      // alias: {
      //   df: path.resolve(process.env.RUNFILES, "df")
      // }
    }

    // plugins: [
    //   new webpack.optimize.LimitChunkCountPlugin({
    //     maxChunks: 1
    //   })
    // ],
  };
  return config;
};
