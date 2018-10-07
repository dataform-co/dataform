var { typescript } = require("../_rules.js");

typescript({
  name: "core",
  build: ctx => [`tsc ${ctx.args}`, `cp parser/index.js build/parser/index.js`],
  deps: ["//typescript:lerna_bootstrap", "//typescript/protos"]
});
