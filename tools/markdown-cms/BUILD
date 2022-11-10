package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "markdown-cms",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:modules-fix",
        "@npm//@octokit/rest",
        "@npm//@types/node",
        "@npm//front-matter",
    ],
)
