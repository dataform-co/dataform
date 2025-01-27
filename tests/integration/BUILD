load("//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_test_suite(
    name = "tests",
    srcs = [
        "bigquery.spec.ts",
        "utils.ts",
    ],
    data = [
        "//test_credentials:bigquery.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
    ],
    tags = ["integration"],
    deps = [
        "//cli/api",
        "//cli/api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
