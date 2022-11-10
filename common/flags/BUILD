package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "flags",
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/long",
        "@npm//@types/node",
    ],
)

load("//testing:build_test.bzl", "build_test")

build_test(
    name = "build_test",
    deps = [":flags"],
)
