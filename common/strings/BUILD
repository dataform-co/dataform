load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "strings",
    srcs = [
        "index.ts",
        "stringifier.ts",
    ],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/long",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = ["stringifier_test.ts"],
    data = [
        "//:node_modules/source-map-support",
    ],
    deps = [
        ":strings",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
