load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "all_projects",
    srcs = glob(
        [
            "**/*.*",
        ],
        exclude = [
            "BUILD",
            "examples_test.ts",
        ],
    ),
)

ts_test_suite(
    name = "tests",
    srcs = ["examples_test.ts"],
    data = [
        ":all_projects",
        ":node_modules",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
        "@nodejs//:node",
    ],
    deps = [
        "//cli/api",
        "//common/protos",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        # The CLI bundle remains at this directory level's node_modules, whereas the core package
        # is copied into each example project's node_modules during testing.
        "//packages/@dataform/cli:package_tar",
        "//packages/@dataform/core:package_tar",
    ],
)
