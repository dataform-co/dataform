load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "markdown_docs",
    srcs = ["index.ts"],
)

ts_test_suite(
    name = "tests",
    srcs = ["index_test.ts"],
    deps = [
        ":markdown_docs",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//testing",
    ],
)
