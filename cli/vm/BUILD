package(default_visibility = ["//visibility:public"])

load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "vm",
    srcs = [
        "compile.ts",
        "jit_worker.ts",
    ],
    deps = [
        "//common/protos",
        "//core",
        "//protos:ts",
        "@npm//@types/glob",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//glob",
        "@npm//semver",
        "@npm//vm2",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "backwards_compat_test.ts",
    ],
    deps = [
        ":vm",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/js-yaml",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
        "@npm//js-yaml",
    ],
)

ts_library(
    name = "compile_loader",
    srcs = [],
    data = [
        ":compile_loader.js",
        ":jit_loader.js",
    ],
    deps = [
        ":vm",
        "@npm//source-map-support",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

nodejs_binary(
    name = "compile",
    data = [
        ":vm",
        "@npm//source-map-support",
    ],
    entry_point = ":compile.ts",
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
)

nodejs_binary(
    name = "jit_worker_bin",
    data = [
        ":vm",
        "@npm//source-map-support",
    ],
    entry_point = ":jit_worker.ts",
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
)
