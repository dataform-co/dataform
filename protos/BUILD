load("@rules_proto//proto:defs.bzl", "proto_library")
load("//tools:ts_proto_library.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "dataform_proto",
    srcs = [
        "configs.proto",
        "core.proto",
        "db_adapter.proto",
        "evaluation.proto",
        "execution.proto",
        "jit.proto",
        "profiles.proto",
    ],
    deps = [
      "@com_google_protobuf//:empty_proto",
      "@com_google_protobuf//:struct_proto",
    ],
)

ts_proto_library(
    name = "ts",
    deps = [
        ":dataform_proto",
    ],
)
