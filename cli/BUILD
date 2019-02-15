package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "cli",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/cli",
    deps = [
        "//api",
        "//core",
        "//protos",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//chokidar",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    bootstrap = ["source-map-support"],
    data = [
        ":cli",
        "@npm//source-map-support",
    ],
    entry_point = "df/cli/index.js",
)
