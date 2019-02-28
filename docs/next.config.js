const withTypescript = require("@zeit/next-typescript");
const remarkHighlight = require("remark-highlight.js");
const remarkSlug = require("remark-slug");
const withMDX = require("@zeit/next-mdx")({
  options: {
    mdPlugins: [remarkHighlight, remarkSlug]
  }
});
const withImages = require("next-images");
const withCSS = require("@zeit/next-css");
const path = require("path");

let config = {
  pageExtensions: ["tsx", "md", "mdx"],
  cssModules: true,
  cssLoaderOptions: {
    importLoaders: 1,
    localIdentName: "[local]___[hash:base64:5]"
  },
  webpack: (config, options) => {
    // This makes sure we can transpile common typescript code.
    options.defaultLoaders.babel.options.configFile = path.join(__dirname, ".babelrc");
    config.module.rules.push({
      test: /\.tsx?$/,
      include: /..\/..\/ts/,
      use: [options.defaultLoaders.babel]
    });
    config.module.rules[config.module.rules.length - 1].include = undefined;
    return config;
  }
};

config = withCSS(config);
config = withMDX(config);
config = withTypescript(config);
config = withImages(config);

module.exports = config;
