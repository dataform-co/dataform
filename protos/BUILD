package(default_visibility = ["//visibility:public"])

proto_library(
    name = "protos_lib",
    srcs = ["core.proto", "profiles.proto"],
)

proto_library(
    name = "protos_schedules_lib",
    srcs = ["schedules.proto"],
)

java_proto_library(
    name = "java",
    deps = [":protos_lib", "protos_schedules_lib"],
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "protobufjs_lib",
    deps = [":protos_lib", ":protos_schedules_lib"],
)

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "protos",
    srcs = glob(["index.ts"]),
    module_name = "@dataform/protos",
    data = [
        "//protos:protobufjs_lib",
    ],
    deps = [
        "//protos:protobufjs_lib",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    srcs = glob(["**/*.proto"]),
    package_layers = [
        "//:common.package.json",
        "protos.package.json",
    ],
    deps = [":protos"],
)
