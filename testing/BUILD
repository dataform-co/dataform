load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["build_test.sh"])

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
        "//protos:ts",
        "@npm//@types/diff",
        "@npm//@types/fs-extra",
        "@npm//@types/js-yaml",
        "@npm//@types/json-stable-stringify",
        "@npm//@types/node",
        "@npm//@types/rimraf",
        "@npm//chalk",
        "@npm//diff",
        "@npm//fs-extra",
        "@npm//js-yaml",
        "@npm//json-stable-stringify",
        "@npm//rimraf",
    ],
)

ts_library(
    name = "run_core",
    srcs = [
        "run_core.ts",
    ],
    deps = [
        "//common/protos",
        "//common/vm:vm_runner",
        "//core",
        "//protos:ts",
        "@npm//@types/fs-extra",
        "@npm//@types/node",
        "@npm//fs-extra",
    ],
)

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
