package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "markdown-cms",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform-tools/markdown-cms",
    deps = [
        "@npm//@octokit/rest",
        "@npm//@types/node",
        "@npm//front-matter",
    ],
)
