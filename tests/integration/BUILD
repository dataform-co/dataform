package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_test_suite")

ts_test_suite(
    srcs = [
        "utils.ts",
        "bigquery.spec.ts",
    ],
    data = [
        "//test_credentials:bigquery.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
    ],
    deps = [
        "//cli/api",
        "//cli/api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
