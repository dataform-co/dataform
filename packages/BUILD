load("//tools:ts_library.bzl", "ts_library")
load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

package(default_visibility = ["//visibility:public"])

exports_files(["rollup.config.js", "rollup_dts.config.js"])

ts_library(
    name = "gen-package-json",
    srcs = ["gen_package_json.ts"],
    deps = [
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//yargs",
    ],
)

nodejs_binary(
    name = "gen-package-json-bin",
    templated_args = ["--node_options=--require=source-map-support/register"],
    data = [
        ":gen-package-json",
        "@npm//source-map-support",
    ],
    entry_point = ":gen_package_json.ts",
)
