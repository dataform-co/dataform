load("//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_test_suite(
    name = "tests",
    srcs = [
        "bigquery.spec.ts",
        "evaluate.spec.ts",
        "execute.spec.ts",
        "jit.spec.ts",
        "property_graph.spec.ts",
        "run.spec.ts",
        "utils.ts",
    ],
    data = [
        "//test_credentials:bigquery.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
        "//packages/@dataform/core:bundle.js",
        "//packages/@dataform/core:package.json",
        "//tests/integration/property_graph_project:files",
        "//tests/integration/property_graph_project:node_modules",
    ],
    tags = ["integration"],
    deps = [
        "//cli:index_test_base",
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
