load("//testing:build_test.bzl", "build_test")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flags",
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
    ],
)

build_test(
    name = "build_test",
    deps = [":flags"],
)
