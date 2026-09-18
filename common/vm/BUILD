package(default_visibility = ["//visibility:public"])

load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "vm_runner",
    srcs = [
        "vm_runner.ts",
    ],
    deps = [
        "@npm//@types/node",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = ["vm_runner_test.ts"],
    deps = [
        ":vm_runner",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

ts_library(
    name = "benchmark_lib",
    srcs = ["vm_runner_benchmark.ts"],
    deps = [
        ":vm_runner",
        "@npm//@types/node",
    ],
)

nodejs_binary(
    name = "benchmark",
    data = [
        ":benchmark_lib",
        "@npm//source-map-support",
    ],
    entry_point = ":vm_runner_benchmark.ts",
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
)
