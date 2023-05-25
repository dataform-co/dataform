load("@rules_proto//proto:defs.bzl", "proto_library")
load("@protobuf//:protobuf.bzl", "py_proto_library")
load("@pip//:requirements.bzl", "requirement")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "dataform_proto",
    srcs = [
        "core.proto",
    ],
)

py_proto_library(
    name = "dataform_py_proto",
    srcs = [
        "core.proto",
    ],
    visibility = ["//visibility:public"],
)
