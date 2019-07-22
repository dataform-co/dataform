package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "assertion_utils",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/assertion_utils",
    deps = [
        "@npm//@types/node",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "assertion_utils.package.json",
    ],
    deps = [":assertion_utils"],
)
