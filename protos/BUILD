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
        "extension.proto",
    ],
    strip_import_prefix = "/protos",
    deps = [
      "@com_google_protobuf//:empty_proto",
      "@com_google_protobuf//:struct_proto",
      "@com_google_protobuf//:timestamp_proto",
    ],
)

ts_proto_library(
    name = "ts",
    module_name = "df/protos/ts",
    module_root = "ts",
    deps = [
        ":dataform_proto",
    ],
)
