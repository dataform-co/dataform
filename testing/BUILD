package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "testing",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "@npm//@types/node",
        "@npm//chalk",
        "@npm//@types/diff",
        "@npm//diff",
        "@npm//@types/json-stable-stringify",
        "@npm//json-stable-stringify",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    deps = [
        ":testing",
        "@npm//@types/diff",
        "@npm//@types/chai",
        "@npm//@types/json-stable-stringify",
        "@npm//json-stable-stringify",
        "@npm//diff",
        "@npm//chai",
    ],
)
