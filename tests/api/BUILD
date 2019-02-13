package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//rimraf",
        "@npm//stack-trace",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "api",
    srcs = [
        ":lib",
    ],
    data = [
        "//examples",
        "@npm//chai",
        "@npm//mocha",
    ],
)
