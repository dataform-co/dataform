load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "errors",
    srcs = ["errors.ts"],
)

ts_test_suite(
    srcs = [
        "errors_test.ts",
    ],
    deps = [
        ":errors",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//testing",
    ],
)
