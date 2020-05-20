load("@df//testing:index.bzl", "ts_test_suite")
load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "strings",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/long",
        "@npm//long",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    data = [
        "@npm//source-map-support",
    ],
    templated_args = ["--node_options=--require=source-map-support/register"],
    deps = [
        "//common/strings",
        "@df//testing",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
