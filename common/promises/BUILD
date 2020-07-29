load("@df//tools:ts_library.bzl", "ts_library")
load("@df//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "promises",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/node",
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
        ":promises",
        "@df//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
