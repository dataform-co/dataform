package(default_visibility=["//visibility:public"])

proto_library(
    name = "protos_lib",
    srcs = glob(["**/*.proto"]),
)

load("//:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "protobufjs_lib",
    deps = [":protos_lib"],
)

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "protos",
    module_name = "@dataform/protos",
    srcs = glob(["index.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//protobufjs",
      "//protos:protobufjs_lib",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "pkg",
    srcs = ["package.json"],
    deps = [":protos"],
)
