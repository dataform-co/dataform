var { typescript } = require("../_rules.js");

typescript({
  name: "api",
  deps: ["//typescript:lerna_bootstrap", "//typescript/protos", "//typescript/core"]
});
