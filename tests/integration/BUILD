package(default_visibility = ["//visibility:public"])

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(
        ["*.ts"],
    ),
    data = [
        "//test_credentials:bigquery.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
    ],
    deps = [
        "//api",
        "//api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
