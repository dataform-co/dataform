const withTypescript = require("@zeit/next-typescript");
const remarkHighlight = require("remark-highlight.js");
const withMDX = require("@zeit/next-mdx")({
  extension: /\.mdx?$/,
  options: {
    mdPlugins: [remarkHighlight]
  }
});
const CopyWebpackPlugin = require("copy-webpack-plugin");

const path = require("path");

module.exports = withTypescript(
  withMDX({
    pageExtensions: ["tsx", "md", "mdx"],
    webpack: (config, { isServer }) => {
      if (!isServer) {
        config.plugins.push(
          new CopyWebpackPlugin([
            {
              from: path.resolve(path.join(__dirname, "../node_modules/@blueprintjs/core/lib/css/blueprint.css")),
              to: path.join(__dirname, "./static/css/")
            },
            {
              from: path.resolve(path.join(__dirname, "../node_modules/highlight.js/styles/atom-one-dark.css")),
              to: path.join(__dirname, "./static/css/highlight.css")
            }
          ])
        );
      }
      return config;
    }
  })
);
