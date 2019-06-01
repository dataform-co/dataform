package(default_visibility = ["//visibility:public"])

proto_library(
    name = "protos_lib",
    srcs = glob(["**/*.proto"]),
)

java_proto_library(
    name = "java",
    deps = [":protos_lib"],
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "protobufjs_lib",
    deps = [":protos_lib"],
)

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "protos",
    srcs = glob(["index.ts"]),
    module_name = "@dataform/protos",
    deps = [
        "//protos:protobufjs_lib",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = ["//:common.package.json", "protos.package.json"],
    srcs = glob(["**/*.proto"]),
    deps = [":protos"],
)
