load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

proto_library(
    name = "foo_proto",
    srcs = ["example1.proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "foo_go_proto",
    importpath = "github.com/dataform-co/dataform/protobufts/examples",
    proto = ":foo_proto",
    visibility = ["//visibility:public"],
)

load("//protobufts:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "foo_ts_proto",
    protos = [
        ":foo_proto",
    ],
)
