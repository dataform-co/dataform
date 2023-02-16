load("//tools:ts_library.bzl", "ts_library")
load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

package(default_visibility = ["//visibility:public"])

exports_files([
    "ts_grpc_service.bzl",
])

ts_library(
    name = "grpc_web_rpc_impl",
    srcs = [
        "grpc_web_rpc_impl.ts",
    ],
    deps = [
        "//:node_modules/grpc-web-client",
        "//:node_modules/protobufjs",
        "//protos:ts",
    ],
)

ts_library(
    name = "generate_service_defs_lib",
    srcs = ["generate_service_defs.ts"],
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/@types/yargs",
        "//:node_modules/path-exists",
        "//:node_modules/protobufjs",
        "//:node_modules/yargs",
    ],
)

nodejs_binary(
    name = "generate_service_defs",
    data = [
        ":generate_service_defs_lib",
        "//:node_modules/source-map-support",
    ],
    entry_point = ":generate_service_defs.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)
