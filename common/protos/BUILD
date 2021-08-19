package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "protos",
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//:modules-fix",
        "//common/strings",
        "@npm//protobufjs",
    ],
)

load("//testing:build_test.bzl", "build_test")

build_test(
    name = "build_test",
    deps = [":protos"],
)
