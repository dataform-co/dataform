package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "core",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/core",
    deps = [
        "//protos",
        "//sqlx",
        "@npm//@types/node",
        "@npm//protobufjs",
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
