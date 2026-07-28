package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")
load("@df//testing:index.bzl", "ts_test_suite")

ts_library(
    name = "flags",
    srcs = glob(
        ["*.ts"],
        exclude = ["*_test.ts", "*.spec.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//long",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*_test.ts"]),
    data = [
        "@npm//source-map-support",
    ],
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
    deps = [
        ":flags",
        "@df//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)

load("//testing:build_test.bzl", "build_test")

build_test(
    name = "build_test",
    deps = [":flags"],
)
