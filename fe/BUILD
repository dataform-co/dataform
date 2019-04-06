package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

filegroup(
    name = "css",
    srcs = glob(["**/*.css"]),
)

css_typings(
    name = "css_typings",
    srcs = [":css"],
)

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "fe",
    srcs = glob([
        "**/*.tsx",
        "**/*.ts",
    ]),
    data = [
        ":css",
    ],
    module_name = "@dataform/fe",
    deps = [
        ":css",
        "@npm//@blueprintjs/core",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//react",
    ],
)
