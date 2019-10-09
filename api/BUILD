package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "lib",
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
    ],
)

ts_library(
    name = "api",
    srcs = [],
    data = [
        ":vm/compile_bin_loader.js",
        ":vm/query_bin_loader.js",
    ],
    deps = [
        ":lib",
        "@npm//source-map-support",
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

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

nodejs_binary(
    name = "vm/compile",
    bootstrap = ["source-map-support/register"],
    data = [
        ":lib",
        "@npm//source-map-support",
    ],
    entry_point = ":vm/compile.ts",
)

nodejs_binary(
    name = "vm/query",
    bootstrap = ["source-map-support/register"],
    data = [
        ":lib",
        "@npm//source-map-support",
    ],
    entry_point = ":vm/query.ts",
)
