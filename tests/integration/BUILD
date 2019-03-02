package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["utils.ts"]),
    deps = [
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/mocha",
        "@npm//@types/node",
    ],
)

ts_library(
    name = "bigquery_lib",
    srcs = glob(["bigquery.spec.ts"]),
    deps = [
        ":utils",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/mocha",
        "@npm//@types/node",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "bigquery",
    srcs = [
        ":bigquery_lib",
    ],
    data = [
        "//examples/bigquery:files",
        "//examples/bigquery:node_modules",
        "//test_profiles",
        "@npm//chai",
        "@npm//mocha",
    ],
)

ts_library(
    name = "redshift_lib",
    srcs = glob(["redshift.spec.ts"]),
    deps = [
        ":utils",
        "@npm//@types/mocha",
        "@npm//@types/node",
    ],
)

mocha_node_test(
    name = "redshift",
    srcs = [
        ":redshift_lib",
    ],
    data = [
        "//examples/redshift:files",
        "//examples/redshift:node_modules",
        "//test_profiles",
        "@npm//chai",
        "@npm//mocha",
    ],
)

ts_library(
    name = "snowflake_lib",
    srcs = glob(["snowflake.spec.ts"]),
    deps = [
        ":utils",
        "@npm//@types/mocha",
        "@npm//@types/node",
    ],
)

mocha_node_test(
    name = "snowflake",
    srcs = [
        ":snowflake_lib",
    ],
    data = [
        "//examples/snowflake:files",
        "//examples/snowflake:node_modules",
        "//test_profiles",
        "@npm//chai",
        "@npm//mocha",
    ],
)
