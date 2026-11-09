load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "utils",
    srcs = glob(
        ["**/*.ts"],
        exclude = [
            "**/*_test.ts",
        ],
    ),
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/object-sizeof",
        "//common/protos",
        "//common/strings",
        "//core",
        "//protos:ts",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "query_transforms_test.ts"
    ],
    deps = [
        ":utils",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/chai",
    ],
)