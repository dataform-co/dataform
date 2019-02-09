package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "api",
    module_name = "@dataform/api",
    srcs = glob(["**/*.ts"]),
    data = ["package.json"],
    deps = [
      "@npm//@types/node",
      "@npm//@types/long",
      "@npm//@google-cloud/bigquery",
      "@npm//node-redshift",
      "@npm//promise-pool-executor",
      "@npm//pretty-ms",
      "@npm//glob",
      "@npm//vm2",
      "@npm//protobufjs",
      "@npm//snowflake-sdk",
      "//core",
      "//protos",
    ]
)
