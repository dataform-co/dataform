package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "sqlx",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:modules-fix",
        "@npm//@types/moo",
        "@npm//@types/node",
        "@npm//moo",
    ],
)
