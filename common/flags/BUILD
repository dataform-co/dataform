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
