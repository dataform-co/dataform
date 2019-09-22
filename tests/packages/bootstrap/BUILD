package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//packages/bootstrap",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//alea",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "bootstrap",
    srcs = [
        ":lib",
    ],
    data = [
        "@npm//chai",
        "@npm//mocha",
    ],
)
