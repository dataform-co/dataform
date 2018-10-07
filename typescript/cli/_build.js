var { typescript } = require("../_rules.js");

typescript({
  name: "cli",
  binary: ctx => `node build/index.js ${ctx.args}`,
  deps: [
    "//typescript:lerna_bootstrap",
    "//typescript/protos",
    "//typescript/core",
    "//typescript/api"
  ]
});
