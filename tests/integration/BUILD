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
        "//packages/@dataform/core:bundle.js",
        "//packages/@dataform/core:package.json",
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
        "@npm//@types/fs-extra",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
    ],
)
