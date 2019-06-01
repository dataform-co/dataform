package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "crossdb",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/crossdb",
    deps = [
        "//core",
        "@npm//@types/node",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = ["//:common.package.json", "crossdb.package.json"],
    deps = [":crossdb"],
)
