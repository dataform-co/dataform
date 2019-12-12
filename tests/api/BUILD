package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

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
        "@npm//@types/stack-trace",
        "@npm//rimraf",
        "@npm//stack-trace",
        "@npm//ts-mockito",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "api",
    srcs = [
        ":lib",
    ],
    data = [
        "//examples/backwards_compatibility:files",
        "//examples/backwards_compatibility:node_modules",
        "//examples/common_v1:files",
        "//examples/common_v1:node_modules",
        "//examples/common_v2:files",
        "//examples/common_v2:node_modules",
        "//examples/formatter:files",
        "//examples/formatter:node_modules",
        "//examples/init",
        "//examples/init:node_modules",
        "//examples/invalid_dataform_json:files",
        "//examples/invalid_dataform_json:node_modules",
        "//examples/never_finishes_compiling:files",
        "//examples/never_finishes_compiling:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)
