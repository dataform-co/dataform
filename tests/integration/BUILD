package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

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
        "//api",
        "//core",
        "//protos",
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
        "//test_profiles",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)

ts_library(
    name = "redshift_lib",
    srcs = glob(["redshift.spec.ts"]),
    deps = [
        ":utils",
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
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
        "//test_profiles",
        "//tests/integration/redshift_project:files",
        "//tests/integration/redshift_project:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)

ts_library(
    name = "snowflake_lib",
    srcs = glob(["snowflake.spec.ts"]),
    deps = [
        ":utils",
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
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
        "//test_profiles",
        "//tests/integration/snowflake_project:files",
        "//tests/integration/snowflake_project:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)
