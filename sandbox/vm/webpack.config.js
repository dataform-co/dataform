const path = require("path");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    entry: [path.resolve(process.env.RUNFILES, "df/sandbox/vm/worker")],
    output: {
      path: path.dirname(path.resolve(argv.output)),
      filename: path.basename(argv.output)
    },
    target: "node",
    optimization: {
      minimize: argv.mode === "production"
    },
    stats: {
      warnings: true
    },
    resolve: {
      extensions: [".ts", ".js"],
      alias: {
        df: path.resolve(process.env.RUNFILES, "df")
      }
    },
    externals: {
      vm2: "require('vm2')"
    }
  };
  return config;
};
