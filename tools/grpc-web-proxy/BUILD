package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "grpc-web-proxy",
    module_name = "@dataform/grpc-web-proxy",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

nodejs_binary(
    name = "cli",
    bootstrap = ["source-map-support/register"],
    data = [
        ":grpc-web-proxy",
        "@npm//source-map-support",
    ],
    entry_point = ":cli.ts",
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "grpc-web-proxy.package.json",
    ],
    deps = [":grpc-web-proxy"],
)
