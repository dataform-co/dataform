load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("//tools:ts_proto_library.bzl", "ts_proto_compile")
load("//testing:build_test.bzl", "build_test")
load("//tools:ts_library.bzl", "ts_library")

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

java_proto_library(
    name = "dataform_java_proto",
    deps = [":dataform_proto"],
)

go_proto_library(
    name = "dataform_go_proto",
    importpath = "github.com/dataform-co/dataform/protos/dataform",
    proto = ":dataform_proto",
)

ts_proto_compile(
    name = "ts_proto",
    output_mode = "NO_PREFIX_FLAT",
    protos = [
        ":dataform_proto",
    ],
    verbose = 4,
)

ts_library(
    name = "ts",
    srcs = [
        ":ts_proto",
    ],
    deps = [
        "//:node_modules/protobufjs",
        "//:node_modules/long",
        "//:node_modules/@types/long",
        # "//:node_modules/@protobuf-ts/runtime",
        # "//:node_modules/@protobuf-ts/runtime-rpc",
        # "//google/protobuf:protobuf_typescript_protos",
    ],
)

build_test(
    name = "build_test",
    deps = [
        ":dataform_go_proto",
        ":dataform_java_proto",
    ],
)
