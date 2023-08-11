package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")
load("@df//tools:css_typings.bzl", "css_typings")
load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")
load("@io_bazel_rules_sass//:defs.bzl", "sass_library")

multi_sass_binary(
    name = "sass",
    srcs = glob(["*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)

css_typings(
    name = "css_typings",
    srcs = [
        ":sass",
    ],
)

ts_library(
    name = "components",
    srcs = glob(["*.tsx"]),
    data = [
        ":sass",
    ],
    deps = [
        ":css_typings",
        "//:modules-fix",
        "@npm//@blueprintjs/core",
        "@npm//@blueprintjs/icons",
        "@npm//@blueprintjs/select",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//@types/react-router",
        "@npm//@types/validator",
        "@npm//re-resizable",
        "@npm//react",
        "@npm//react-router",
        "@npm//validator",
    ],
)
