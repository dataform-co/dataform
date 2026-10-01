load("@aspect_rules_js//js:defs.bzl", "js_library")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//visibility:private"])

js_library(
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
        "//packages/@dataform/cli:bundle",
        "//packages/@dataform/cli:json",
        "//packages/@dataform/cli:worker_bundle",
    ],
    deps = [
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//cli:index_test_base",
        "//cli/api",
        "//common/protos",
        "//protos:ts",
        "//testing",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
    ],
)
