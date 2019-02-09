package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "cli",
    module_name = "@dataform/cli",
    srcs = glob(["**/*.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/long",
      "@npm//@types/yargs",
      "@npm//chokidar",
      "@npm//yargs",
      "//api",
      "//core",
      "//protos",
    ]
)

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    data = [
      ":cli",
      "@npm//source-map-support",
    ],
    entry_point = "df/cli/index.js",
    bootstrap = ["source-map-support"]
)
