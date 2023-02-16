load("//tools:ts_library.bzl", "ts_library")
load("@aspect_rules_js//js:defs.bzl", "js_binary")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "json-merge",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//yargs",
    ],
)

js_binary(
    name = "bin",
    data = [
        ":json-merge",
        "@npm//source-map-support",
    ],
    entry_point = ":index.ts",
    node_options = ["--require=source-map-support/register"],
)
