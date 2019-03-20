package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    data = [
        "//api:vm/compile",
    ],
    deps = [
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//rimraf",
        "@npm//stack-trace",
        "@npm//ts-mockito",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "api",
    srcs = [
        ":lib",
    ],
    data = [
        "//examples/bigquery:files",
        "//examples/bigquery:node_modules",
        "//examples/bigquery_backwards_compatibility:files",
        "//examples/bigquery_backwards_compatibility:node_modules",
        "//examples/bigquery_with_errors:files",
        "//examples/bigquery_with_errors:node_modules",
        "//examples/init",
        "//examples/init:node_modules",
        "//examples/redshift:files",
        "//examples/redshift:node_modules",
        "//examples/redshift_operations:files",
        "//examples/redshift_operations:node_modules",
        "//examples/snowflake:files",
        "//examples/snowflake:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)
