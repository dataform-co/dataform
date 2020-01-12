package(default_visibility = ["//tests:__subpackages__"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//testing",
        "@npm//@types/node",
        "@npm//@types/rimraf",
        "@npm//rimraf",
    ],
)
