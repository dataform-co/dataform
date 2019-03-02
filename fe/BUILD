package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

filegroup(
    name = "files",
    srcs = glob(["**/*"]),
)

css_typings(
    name = "css",
    srcs = glob(["**/*.css"]),
    path = "fe",
)

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "fe",
    srcs = glob(["**/*.tsx"]) + [":css"],
    module_name = "@dataform/fe",
    deps = [
        "@npm//@blueprintjs/core",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//react",
    ],
)
