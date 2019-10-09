package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//api",
        "//core",
        "//protos",
        "@npm//chai",
        "@npm//mocha",
        "@npm//@types/chai",
        "@npm//@types/mocha",
        "@npm//@types/node",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "redshift",
    srcs = [
        ":lib",
    ],
    data = [
        "//test_credentials",
        "//examples/redshift:files",
        "//examples/redshift:node_modules",
    ],
)
