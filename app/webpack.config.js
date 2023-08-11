const path = require("path");
const webpack = require("webpack");
const fs = require("fs");

module.exports = (env, argv) => {
  const config = {
    mode: argv.mode || "development",
    entry: [path.resolve(process.env.RUNFILES, "df/app/index")],
    output: {
      path: path.dirname(path.resolve(argv.output)),
      filename: path.basename(argv.output)
    },
    optimization: {
      minimize: argv.mode === "production"
    },
    watchOptions: {
      ignored: [/node_modules/]
    },
    stats: {
      warnings: true
    },
    node: {
      fs: "empty",
      child_process: "empty"
    },
    devServer: {
      port: 9110,
      open: false,
      publicPath: "/",
      contentBase: path.resolve("./app"),
      stats: {
        colors: true
      },
      after: (_, socket) => {
        // Listen to STDIN, which is written to by ibazel to tell it to reload.
        // Must check the message so we only bundle after a successful build completes.
        process.stdin.on("data", data => {
          if (!String(data).includes("IBAZEL_BUILD_COMPLETED SUCCESS")) {
            return;
          }
          socket.sockWrite(socket.sockets, "content-changed");
        });
      }
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
    module: {
      rules: [
        {
          test: /\.css$/,
          include: /node_modules/,
          use: ["style-loader", "css-loader"]
        },
        {
          test: /\.css$/,
          exclude: /node_modules/,
          use: [
            { loader: "style-loader" },
            {
              loader: "css-loader",
              query: {
                modules: true
              }
            }
          ]
        }
      ]
    }
  };
  // Only add the babel transpilation in production mode.
  if (argv.mode === "production") {
    config.module.rules.push({
      test: /\.jsx?$/,
      exclude: /node_modules/,
      use: [
        {
          loader: "babel-loader",
          options: {
            presets: [["env", { targets: { browsers: ["defaults"] } }]],
            plugins: [
              [
                "transform-runtime",
                {
                  polyfill: false,
                  regenerator: true
                }
              ]
            ]
          }
        }
      ]
    });
  }
  return config;
};
