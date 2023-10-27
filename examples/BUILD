load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "all_projects",
    srcs = glob([
        "**/*.*",
    ], exclude = ["BUILD", "examples_test.ts"]),
)

ts_test_suite(
    name = "tests",
    srcs = ["examples_test.ts"],
    data = [
        ":all_projects",
        ":node_modules",
    ],
    deps = [
        "//api",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//chai",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
    ],
    for_subdirectory = "stackoverflow_reporter",
)
