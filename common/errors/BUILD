load("@df//testing:index.bzl", "ts_test_suite")
load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "errors",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    data = [
        "@npm//source-map-support",
    ],
    templated_args = ["--node_options=--require=source-map-support/register"],
    deps = [
        ":errors",
        "@df//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
