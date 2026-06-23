load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "testing",
    testonly = 1,
    srcs = glob(
        ["*.ts"],
    ),
    deps = [
        "//common/flags",
        "//testing",
    ],
)
