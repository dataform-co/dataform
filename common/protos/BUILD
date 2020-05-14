load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "protos",
    srcs = glob(["*.ts"]),
    deps = [
        "@npm//protobufjs",
    ],
)
