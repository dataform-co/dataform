load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "files",
    srcs = glob(["**/*.proto"]),
)

proto_library(
    name = "dataform_proto",
    srcs = [
        "core.proto",
        "evaluation.proto",
        "execution.proto",
        "profiles.proto",
    ],
)

py_proto_library(
    name = "dataform_py_proto",
    srcs = [
        "core.proto",
    ],
    visibility = ["//visibility:public"],
)
