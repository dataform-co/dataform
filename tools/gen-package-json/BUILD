package(default_visibility = ["//visibility:public"])

load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "json-merge",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/@types/yargs",
        "//:node_modules/yargs",
    ],
)

js_binary(
    name = "bin",
    data = [
        ":json-merge",
        "//:node_modules/source-map-support",
    ],
    entry_point = ":index.ts",
    args = ["--bazel_patch_module_resolver"],
    node_options = ["--require=source-map-support/register"],
)
