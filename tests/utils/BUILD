package(default_visibility = ["//tests:__subpackages__"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//rimraf",
    ],
)
