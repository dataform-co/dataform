load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["reserializer.go"],
    importpath = "github.com/dataform-co/dataform/protobufts/tests/reserializer",
    visibility = ["//visibility:private"],
    deps = [
        "//protobufts/tests:testprotos_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_binary(
    name = "reserializer",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
