load("@rules_proto//proto:defs.bzl", "proto_library")
load("//tools:ts_proto_library.bzl", "ts_proto_library")
load("//testing:build_test.bzl", "build_test")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "dataform_proto",
    srcs = [
        "configs.proto",
        "core.proto",
        "evaluation.proto",
        "execution.proto",
        "profiles.proto",
    ],
)

ts_proto_library(
    name = "ts",
    deps = [
        ":dataform_proto",
    ],
)
