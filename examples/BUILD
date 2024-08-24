load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")
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
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
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
