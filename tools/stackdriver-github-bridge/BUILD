package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "stackdriver-github-bridge",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@octokit/rest",
        "@npm//@types/express",
        "@npm//@types/node",
        "@npm//@types/request",
        "@npm//express",
        "@npm//request",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "pkg_npm")

pkg_npm(
    name = "package",
    srcs = ["package.json"],
    deps = [":stackdriver-github-bridge"],
)

load("//tools/gcloud:gcloud_function.bzl", "deploy_http_nodejs_gcloud_function")

deploy_http_nodejs_gcloud_function(
    name = "deploy",
    env_vars_file = "env.yaml",
    function_name = "handleStackdriverEvent",
    pkg_npm = ":package",
)
