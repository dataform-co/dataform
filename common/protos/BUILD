load("//testing:build_test.bzl", "build_test")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "protos",
    srcs = ["index.ts"],
    deps = [
        "//common/strings",
        "//:node_modules/protobufjs",
    ],
)

build_test(
    name = "build_test",
    deps = [":protos"],
)
