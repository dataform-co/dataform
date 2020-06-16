const withImages = require("next-images");
const withCSS = require("@zeit/next-css");
const path = require("path");
const fs = require("fs");

let config = {
  pageExtensions: ["tsx"],
  cssModules: true,
  // Next cannot handle absolute distDir's, so go up to the top of the workspace.
  distDir: `../${process.argv.slice(-1)[0]}`,
  cssLoaderOptions: {
    importLoaders: 1,
    localIdentName: "[name]___[hash:base64:5]"
  },
  webpack: (config, options) => {
    // Use the module name mappings in tsconfig so imports resolve properly.
    config.resolve.plugins = config.resolve.plugins || [];

    config.resolve.alias = {
      ...config.resolve.alias,
      df: path.resolve(path.join(process.env.RUNFILES, "df")),
    };

    config.module.rules.push({
      test: /\.[tj]sx?$/,
      exclude: /node_modules/,
      use: [options.defaultLoaders.babel]
    });
  
    // Tell webpack to preserve information about file names so we can use them for paths.
    config.node = { __filename: true, fs: "empty", child_process: "empty" };
    return config;
  }
};

config = withCSS(config);
config = withImages(config);

module.exports = config;
