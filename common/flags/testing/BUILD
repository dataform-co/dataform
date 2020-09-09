package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "testing",
    testonly = 1,
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//:modules-fix",
        "//common/flags",
        "//testing",
    ],
)
