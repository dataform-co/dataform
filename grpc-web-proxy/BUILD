package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "grpc-web-proxy",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/bunyan",
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//bunyan",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    data = [
        ":grpc-web-proxy",
        "@npm//source-map-support",
    ],
    entry_point = ":cli.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)
