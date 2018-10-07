module.exports = {
  typescript: options =>
    rule(
      Object.assign(
        {
          build: ctx => [`tsc ${ctx.args}`],
          cache: {
            include: "**/*.ts",
            exclude: "+(node_modules|build)/**"
          }
        },
        options
      )
    )
};
