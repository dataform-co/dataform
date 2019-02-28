package(default_visibility = ["//visibility:public"])

proto_library(
    name = "protos_lib",
    srcs = glob(["**/*.proto"]),
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "protobufjs_lib",
    deps = [":protos_lib"],
)

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

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

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"] + glob(["**/*.proto"]),
    deps = [":protos"],
)

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "package_tar",
    extension = "tgz",
    srcs = [
        ":package",
    ],
)
