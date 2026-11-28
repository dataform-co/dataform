load("@rules_proto//proto:defs.bzl", "proto_library")
load("//testing:build_test.bzl", "build_test")
load("//tools:ts_proto_library.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "proto_files",
    srcs = glob(["*.proto"]),
)

_PROTO_DEPS = {
    # Intentionally excludes `core_proto` so `configs.proto` remains decoupled
    # from compiled-graph protobufs under `protoc --direct_dependencies`.
    "configs.proto": [
        ":extension_proto",
        ":property_graph_config_proto",
        "@com_google_protobuf//:struct_proto",
    ],
    "core.proto": [
        ":extension_proto",
        "@com_google_protobuf//:struct_proto",
    ],
    "db_adapter.proto": [
        ":core_proto",
    ],
    "evaluation.proto": [],
    "execution.proto": [
        ":core_proto",
        ":db_adapter_proto",
        "@com_google_protobuf//:struct_proto",
    ],
    "extension.proto": [],
    "jit.proto": [
        ":core_proto",
        ":db_adapter_proto",
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:struct_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
    "profiles.proto": [],
    "property_graph_config.proto": [
        "@com_google_protobuf//:struct_proto",
    ],
}

[
    fail("Add %s to _PROTO_DEPS in protos/BUILD so protoc validates its imports." % src)
    for src in glob(["*.proto"])
    if src not in _PROTO_DEPS
]

# Private per-file targets used only by `:build_test` to validate direct imports;
# kept private so downstream targets cannot link both a per-file target and `:dataform_proto`.
[
    proto_library(
        name = src.removesuffix(".proto") + "_proto",
        srcs = [src],
        strip_import_prefix = "/protos",
        visibility = ["//visibility:private"],
        deps = deps,
    )
    for src, deps in _PROTO_DEPS.items()
]

# Stages all `.proto` sources into a single `_virtual_imports/dataform_proto/`
# directory so `pbjs` in `ts_proto_library` can resolve sibling relative imports.
proto_library(
    name = "dataform_proto",
    srcs = list(_PROTO_DEPS.keys()),
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
    deps = [":" + src.removesuffix(".proto") + "_proto" for src in _PROTO_DEPS] + [
        ":dataform_proto",
        ":ts",
    ],
)
