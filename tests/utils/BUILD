package(default_visibility = ["//tests:__subpackages__"])

load("//tools:ts_library.bzl", "ts_library")

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
