load("@rules_proto//proto:defs.bzl", "proto_library")
load("@aspect_rules_ts//ts:proto.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "dataform_proto",
    srcs = [
        "configs.proto",
        "core.proto",
        "data_preparation.proto",
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
