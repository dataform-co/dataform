package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")
load("@df//tools:css_typings.bzl", "css_typings")
load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")

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
    name = "charts",
    srcs = glob(["*.tsx"]),
    data = [
        ":sass",
    ],
    deps = [
        ":css_typings",
        "//:modules-fix",
        "@npm//@nivo/bar",
        "@npm//@nivo/core",
        "@npm//@nivo/line",
        "@npm//@types/react",
        "@npm//@types/react-router",
        "@npm//react",
        "@npm//react-router",
    ],
)
