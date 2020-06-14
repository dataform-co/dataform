package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "runtime",
    srcs = glob(["*.ts"]),
    deps = [
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)
