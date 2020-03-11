package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

filegroup(
    name = "files",
    srcs = glob(["**/*.*"]),
)

ts_library(
    name = "core",
    srcs = glob(["**/*.ts"]),
    data = [
        ":package.json",
    ],
    module_name = "@dataform/core",
    deps = [
        "//protos",
        "//sqlx",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//@types/stack-trace",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//stack-trace",
        "@npm//tarjan-graph",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "core.package.json",
    ],
    deps = [":core"],
)
