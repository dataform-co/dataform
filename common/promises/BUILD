load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "promises",
    srcs = ["index.ts"],
    deps = [
        "//:node_modules/@types/node",
    ],
)

ts_test_suite(
    srcs = ["index_test.ts"],
    data = [
        "//:node_modules/source-map-support",
    ],
    deps = [
        ":promises",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
