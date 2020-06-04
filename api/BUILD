package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//common/errors",
        "//common/strings",
        "//core",
        "//protos:ts",
        "//sqlx",
        "//api/utils",
        "@npm//@google-cloud/bigquery",
        "@npm//@types/glob",
        "@npm//@types/js-beautify",
        "@npm//@types/lodash",
        "@npm//@types/long",
        "@npm//@types/mssql",
        "@npm//@types/node",
        "@npm//@types/object-hash",
        "@npm//@types/pg",
        "@npm//@types/semver",
        "@npm//@types/ssh2",
        "@npm//cron-parser",
        "@npm//get-port",
        "@npm//glob",
        "@npm//js-beautify",
        "@npm//lodash",
        "@npm//mssql",
        "@npm//object-hash",
        "@npm//pg",
        "@npm//pg-cursor",
        "@npm//pretty-ms",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//snowflake-sdk",
        "@npm//sql-formatter",
        "@npm//ssh2",
        "@npm//vm2",
    ],
)

ts_library(
    name = "api",
    srcs = [],
    data = [
        ":vm/compile_loader.js",
    ],
    deps = [
        ":lib",
        "@npm//source-map-support",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

nodejs_binary(
    name = "vm/compile",
    data = [
        ":lib",
        "@npm//source-map-support",
    ],
    entry_point = ":vm/compile.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)
