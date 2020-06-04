package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//common/strings",
        "//protos:ts",
        "@npm//@types/node",
        "@npm//@types/object-hash",
        "@npm//object-hash",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")
