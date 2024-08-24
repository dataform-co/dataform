load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_library")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@aspect_rules_ts//ts:defs.bzl", "ts_config")

package(default_visibility = ["//visibility:public"])

npm_link_all_packages(name = "node_modules")

exports_files([
    "package.json",
    "readme.md",
    "version.bzl",
    "jsconfig.json"
])

js_library(
    name = "package_json",
    srcs = ["package.json"],
    visibility = ["//visibility:public"],
)

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
