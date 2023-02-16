load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("@bazel_gazelle//:def.bzl", "gazelle")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@aspect_rules_ts//ts:defs.bzl", "ts_config")

package(default_visibility = ["//visibility:public"])

npm_link_all_packages(name = "node_modules")

exports_files([
    "package.json",
    "readme.md",
    "version.bzl",
])

ts_config(
    name = "tsconfig",
    src = "tsconfig.json",
)

js_binary(
    name = "run_tslint",
    entry_point = "//:node_modules/tslint/bin/tslint",
    node_options = ["--preserve-symlinks"],
    # install_source_map_support = False,
)

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
