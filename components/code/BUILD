package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "code",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:modules-fix",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//react",
        "@npm//react-syntax-highlighter",
        "@npm//@types/react-syntax-highlighter"
    ],
)
