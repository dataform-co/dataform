load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["protomongo_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//protos:dataform_go_proto",
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
