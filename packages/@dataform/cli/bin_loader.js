// Dev entry point for `bazel run //packages/@dataform/cli:bin`: install the "df/..." resolver
// patch (js_binary sets no node_options here), then start the CLI.
require("../../../testing/resolver-patch.js");
require("./index.js");
