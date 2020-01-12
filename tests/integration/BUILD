package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = ["utils.ts"],
    deps = [
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/node",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    srcs = glob(["*.spec.ts"]),
    data = [
        "//test_credentials",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
        "//tests/integration/redshift_project:files",
        "//tests/integration/redshift_project:node_modules",
        "//tests/integration/snowflake_project:files",
        "//tests/integration/snowflake_project:node_modules",
        "//tests/integration/sqldatawarehouse_project:files",
        "//tests/integration/sqldatawarehouse_project:node_modules",
    ],
    deps = [
        ":utils",
        "//api",
        "//core",
        "//protos",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
