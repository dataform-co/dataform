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
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//cli:index_test_base",
        "//cli/api",
        "//cli/api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)
