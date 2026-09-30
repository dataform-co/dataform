load("//testing:build_test.bzl", "build_test")
load("//tools:ts_library.bzl", "ts_library")
load("@df//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flags",
    srcs = glob(
        ["*.ts"],
        exclude = ["*_test.ts", "*.spec.ts"],
    ),
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/long",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*_test.ts"]),
    data = [
        "//:node_modules/source-map-support",
    ],
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
    deps = [
        ":flags",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "@df//testing",
    ],
)

build_test(
    name = "build_test",
    deps = [":flags"],
)
