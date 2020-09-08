load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["reserializer.go"],
    importpath = "github.com/dataform-co/dataform/protobufts/tests/reserializer",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "reserializer",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
