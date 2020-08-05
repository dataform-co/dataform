package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//common/strings",
        "//protos:ts",
        "@npm//@types/node",
        "@npm//@types/object-hash",
        "@npm//object-hash",
        "@npm//object-sizeof",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")
