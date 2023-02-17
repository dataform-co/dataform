load("//tools:ts_library.bzl", "ts_library")
load("//testing:build_test.bzl", "build_test")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "protos",
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//:node_modules/protobufjs",
        "//common/strings",
    ],
)

build_test(
    name = "build_test",
    deps = [":protos"],
)
