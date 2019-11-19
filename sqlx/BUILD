package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "sqlx",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/sqlx",
    deps = [
        "@npm//@types/moo",
        "@npm//@types/node",
        "@npm//moo",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "sqlx.package.json",
    ],
    deps = [":sqlx"],
)
