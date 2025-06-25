load("//tools:ts_library.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_test_suite(
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
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
