load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["prototester.go"],
    importpath = "github.com/dataform-co/dataform/protobufts/tests/prototester",
    visibility = ["//visibility:private"],
    deps = [
        "//protobufts/tests:testprotos_go_proto",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_binary(
    name = "prototester",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
