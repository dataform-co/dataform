const withTypescript = require("@zeit/next-typescript");
const rehypePrism = require("@mapbox/rehype-prism");
const remarkSlug = require("remark-slug");
const withMDX = require("@zeit/next-mdx")({
  options: {
    hastPlugins: [rehypePrism],
    mdPlugins: [remarkSlug]
  }
});
const withImages = require("next-images");
const withCSS = require("@zeit/next-css");
const path = require("path");
const TsconfigPathsPlugin = require("tsconfig-paths-webpack-plugin");

let config = {
  pageExtensions: ["tsx", "md", "mdx"],
  cssModules: true,
  cssLoaderOptions: {
    importLoaders: 1,
    localIdentName: "[local]___[hash:base64:5]"
  },
  webpack: (config, options) => {
    // Use the module name mappings in tsconfig so imports resolve properly.
    config.resolve.plugins = config.resolve.plugins || [];
    config.resolve.plugins.push(
      new TsconfigPathsPlugin({ extensions: [".ts", ".tsx", ".js", ".css"] })
    );
    // Make sure webpack can resolve modules that live within our bazel managed deps.
    const bazelNodeModulesPath = path.resolve("./external/npm/node_modules");
    config.resolve.modules.push(bazelNodeModulesPath);
    config.resolveLoader.modules.push(bazelNodeModulesPath);
    // Inline babel config for typescript compilation.
    options.defaultLoaders.babel.options.configFile = false;
    options.defaultLoaders.babel.options.presets = [
      require.resolve("next/babel"),
      [require.resolve("@zeit/next-typescript/babel"), { extensions: [".ts", ".tsx", ".mdx"] }]
    ];
    // Another hack for bazel to make sure our typescript code in here actually gets compiled.
    config.module.rules.push({
      test: /\.tsx?$/,
      use: [options.defaultLoaders.babel]
    });
    // Make sure the outputs of other ts_library rules can be consumed without webpack warnings.
    config.module.rules.push({
      test: /\.jsx?$/,
      exclude: /node_modules/,
      use: [{ loader: require.resolve("umd-compat-loader") }]
    });
    return config;
  }
};

config = withCSS(config);
config = withImages(config);
config = withMDX(config);
config = withTypescript(config);

module.exports = config;
