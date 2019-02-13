package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "api",
    srcs = glob(["**/*.ts"]),
    data = ["package.json"],
    module_name = "@dataform/api",
    deps = [
        "//core",
        "//protos",
        "@npm//@google-cloud/bigquery",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//glob",
        "@npm//node-redshift",
        "@npm//pretty-ms",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//snowflake-sdk",
        "@npm//vm2",
    ],
)
