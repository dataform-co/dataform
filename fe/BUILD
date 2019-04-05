package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

filegroup(
    name = "files",
    srcs = glob(["**/*"]),
)

css_typings(
    name = "css",
    srcs = glob(["**/*.css"]),
)

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "fe",
    srcs = glob(["**/*.tsx", "**/*.ts"]),
    module_name = "@dataform/fe",
    deps = [
        ":css",
        "@npm//@blueprintjs/core",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//react",
    ],
)
