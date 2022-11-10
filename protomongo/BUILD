load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["protomongo_test.go"],
    embed = [":go_default_library"],
    deps = [
        ":example_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_mongodb_go_mongo_driver//bson:go_default_library",
    ],
)

go_library(
    name = "go_default_library",
    srcs = ["protomongo.go"],
    importpath = "github.com/dataform-co/dataform/protomongo",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_golang_protobuf//descriptor:go_default_library_gen",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_mongodb_go_mongo_driver//bson/bsoncodec:go_default_library",
        "@org_mongodb_go_mongo_driver//bson/bsonrw:go_default_library",
    ],
)

proto_library(
    name = "example_proto",
    testonly = 1,
    srcs = ["example.proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "example_go_proto",
    testonly = 1,
    importpath = "github.com/dataform-co/dataform/protomongo/example",
    proto = ":example_proto",
    visibility = ["//visibility:public"],
)
