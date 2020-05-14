load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")
load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

multi_sass_binary(
    name = "sass",
    srcs = glob(["*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)

ts_library(
    name = "code",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:modules-fix",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//@types/react-syntax-highlighter",
        "@npm//react",
        "@npm//react-syntax-highlighter",
    ],
)
