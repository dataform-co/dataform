load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/object-sizeof",
        "//common/protos",
        "//common/strings",
        "//core",
        "//protos:ts",
    ],
)
