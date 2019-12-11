package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "api",
    srcs = glob(["**/*.ts"]),
    data = ["package.json"],
    module_name = "@dataform/api",
    deps = [
        "//core",
        "//protos",
        "//sqlx",
        "@npm//@google-cloud/bigquery",
        "@npm//@types/glob",
        "@npm//@types/js-beautify",
        "@npm//@types/long",
        "@npm//@types/mssql",
        "@npm//@types/node",
        "@npm//@types/pg",
        "@npm//cron-parser",
        "@npm//glob",
        "@npm//js-beautify",
        "@npm//mssql",
        "@npm//pg",
        "@npm//pg-cursor",
        "@npm//pretty-ms",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//snowflake-sdk",
        "@npm//sql-formatter",
        "@npm//vm2",
        "@npm//source-map-support"
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "api.package.json",
    ],
    deps = [":api"],
)

