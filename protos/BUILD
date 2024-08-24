load("@rules_proto//proto:defs.bzl", "proto_library")
load("@aspect_rules_ts//ts:proto.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_files = [
    "configs.proto",
    "core.proto",
    "data_preparation.proto",
    "evaluation.proto",
    "execution.proto",
    "profiles.proto",
]

proto_library(
    name = "dataform_proto",
    srcs = proto_files,
)

ts_proto_library(
    name = "ts",
    node_modules = "//:node_modules",
    proto = ":dataform_proto",
    proto_srcs = proto_files
)
