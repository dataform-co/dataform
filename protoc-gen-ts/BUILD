package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "protoc-gen-ts",
    srcs = glob(["*.ts"]),
    deps = [
        ":ts-protoc-protos",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    data = [
        ":protoc-gen-ts",
        "@npm//source-map-support",
    ],
    entry_point = ":protoc-gen-ts.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "ts-protoc-protos",
    deps = [
        "@com_google_protobuf//:descriptor_proto",
        "@com_google_protobuf//:compiler_plugin_proto",
    ],
)
