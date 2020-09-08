load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "protoc-gen-ts",
    srcs = ["protoc-gen-ts.ts"],
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

proto_library(
    name = "descriptor_proto",
    srcs = ["descriptor.proto"],
)

proto_library(
    name = "plugin_go_proto",
    srcs = ["plugin.proto"],
    deps = [":descriptor_proto"],
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "ts-protoc-protos",
    deps = [
        ":descriptor_proto",
        ":plugin_go_proto",
    ],
)

go_proto_library(
    name = "descriptor_go_proto",
    importpath = "github.com/golang/protobuf/protoc-gen-go/descriptor",
    proto = ":descriptor_proto",
)

go_proto_library(
    name = "plugin_go_go_proto",
    importpath = "github.com/golang/protobuf/protoc-gen-go/plugin",
    proto = ":plugin_go_proto",
    deps = [":descriptor_go_proto"],
)
