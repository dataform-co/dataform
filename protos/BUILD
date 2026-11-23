load("@rules_proto//proto:defs.bzl", "proto_library")
load("//testing:build_test.bzl", "build_test")
load("//tools:ts_proto_library.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "proto_files",
    srcs = glob(["*.proto"]),
)

proto_library(
    name = "extension_proto",
    srcs = ["extension.proto"],
    strip_import_prefix = "/protos",
)

proto_library(
    name = "property_graph_config_proto",
    srcs = ["property_graph_config.proto"],
    strip_import_prefix = "/protos",
    deps = [
        "@com_google_protobuf//:struct_proto",
    ],
)

# Intentionally excludes `core_proto` so `configs.proto` remains decoupled from
# compiled-graph protobufs under `protoc --direct_dependencies`.
proto_library(
    name = "configs_proto",
    srcs = ["configs.proto"],
    strip_import_prefix = "/protos",
    deps = [
        ":extension_proto",
        ":property_graph_config_proto",
        "@com_google_protobuf//:struct_proto",
    ],
)

proto_library(
    name = "core_proto",
    srcs = ["core.proto"],
    strip_import_prefix = "/protos",
    deps = [
        ":extension_proto",
        "@com_google_protobuf//:struct_proto",
    ],
)

proto_library(
    name = "db_adapter_proto",
    srcs = ["db_adapter.proto"],
    strip_import_prefix = "/protos",
    deps = [
        ":core_proto",
    ],
)

proto_library(
    name = "evaluation_proto",
    srcs = ["evaluation.proto"],
    strip_import_prefix = "/protos",
)

proto_library(
    name = "execution_proto",
    srcs = ["execution.proto"],
    strip_import_prefix = "/protos",
    deps = [
        ":core_proto",
        ":db_adapter_proto",
        "@com_google_protobuf//:struct_proto",
    ],
)

proto_library(
    name = "jit_proto",
    srcs = ["jit.proto"],
    strip_import_prefix = "/protos",
    deps = [
        ":core_proto",
        ":db_adapter_proto",
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:struct_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "profiles_proto",
    srcs = ["profiles.proto"],
    strip_import_prefix = "/protos",
)

# Stages all `.proto` sources into a single `_virtual_imports/dataform_proto/`
# directory so `pbjs` in `ts_proto_library` can resolve sibling relative imports.
proto_library(
    name = "dataform_proto",
    srcs = glob(["*.proto"]),
    strip_import_prefix = "/protos",
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:struct_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

ts_proto_library(
    name = "ts",
    deps = [
        ":dataform_proto",
    ],
)

build_test(
    name = "build_test",
    deps = [
        ":configs_proto",
        ":core_proto",
        ":dataform_proto",
        ":db_adapter_proto",
        ":evaluation_proto",
        ":execution_proto",
        ":extension_proto",
        ":jit_proto",
        ":profiles_proto",
        ":property_graph_config_proto",
        ":ts",
    ],
)
