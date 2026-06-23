load("//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_test_suite(
    name = "tests",
    srcs = [
        "bigquery.spec.ts",
        "utils.ts",
    ],
    data = [
        "//cli/vm:bazel_compile_loader",
        "//cli/vm:bazel_jit_worker_loader",
        "//test_credentials:bigquery.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
    ],
    tags = ["integration"],
    deps = [
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//cli/api",
        "//cli/api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)
