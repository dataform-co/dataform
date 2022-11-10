package(default_visibility = ["//visibility:public"])

load("//tools/protobufjs:ts_grpc_service.bzl", "ts_grpc_service")

# This spits out TS files that we can use as srcs.
ts_grpc_service(
    name = "grpc_service",
    srcs = ["//protos:files"],
    protos_import = "df/protos/ts",
    root_name = "protos",
    root_paths = ["."],
    service = "dataform.server.Service",
)

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "server",
    srcs = glob(["*.ts"]) + [":grpc_service"],
    data = [
        "//app:app.bundle.js",
        "//app:assets",
    ],
    deps = [
        "//api",
        "//core",
        "//protos:ts",
        "//grpc-web-proxy",
        "@npm//@types/express",
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//express",
        "@npm//grpc",
        "@npm//protobufjs",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    data = [
        ":server",
        "@npm//source-map-support",
    ],
    entry_point = ":index.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)
