load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vm_runner",
    srcs = [
        "module_resolver.ts",
        "path_utils.ts",
        "vm_runner.ts",
    ],
    deps = [
        "//:node_modules/@types/node",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "module_resolver_test.ts",
        "vm_runner_test.ts",
    ],
    deps = [
        ":vm_runner",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//testing",
    ],
)

ts_library(
    name = "benchmark_lib",
    srcs = ["vm_runner_benchmark.ts"],
    deps = [
        ":vm_runner",
        "//:node_modules/@types/node",
    ],
)

# bazel run //common/vm:benchmark
js_binary(
    name = "benchmark",
    data = [
        ":benchmark_lib",
        "//:node_modules/source-map-support",
        "//testing:resolver-patch",
    ],
    entry_point = ":vm_runner_benchmark.js",
    node_options = [
        "--require=./testing/resolver-patch.js",
        "--require=source-map-support/register",
    ],
)
