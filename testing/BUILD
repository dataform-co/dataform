package(default_visibility = ["//visibility:public"])

exports_files(["build_test.sh"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "testing",
    srcs = [
        "child_process.ts",
        "fixtures.ts",
        "hook.ts",
        "index.ts",
        "runner.ts",
        "suite.ts",
        "test.ts",
    ],
    deps = [
        "//:modules-fix",
        "@npm//@types/diff",
        "@npm//@types/json-stable-stringify",
        "@npm//@types/node",
        "@npm//@types/rimraf",
        "@npm//chalk",
        "@npm//diff",
        "@npm//json-stable-stringify",
        "@npm//rimraf",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = ["index_test.ts"],
    deps = [
        ":testing",
        "@npm//@types/chai",
        "@npm//@types/diff",
        "@npm//@types/json-stable-stringify",
        "@npm//chai",
        "@npm//diff",
        "@npm//json-stable-stringify",
    ],
)
