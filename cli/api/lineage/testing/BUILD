package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "testing",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@google-cloud/lineage",
        "@npm//@types/node",
    ],
)
