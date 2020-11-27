package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//common/protos",
        "//common/strings",
        "//protos:ts",
        "@npm//@types/node",
        "@npm//@types/object-hash",
        "@npm//@types/pg",
        "@npm//@types/pg-query-stream",
        "@npm//object-hash",
        "@npm//object-sizeof",
        "@npm//pg",
        "@npm//pg-query-stream",
    ],
)
