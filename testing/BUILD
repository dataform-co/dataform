package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "testing",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    module_name = "@dataform/testing",
    deps = [
        "@npm//@types/node",
        "@npm//chalk",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    deps = [
        ":testing",
        "@npm//@types/chai",
        "@npm//chai",
    ],
)
