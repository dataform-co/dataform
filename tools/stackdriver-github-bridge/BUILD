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

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"],
    deps = [":stackdriver-github-bridge"],
)

load("//tools/gcloud:gcloud_function.bzl", "nodejs_gcloud_function")

nodejs_gcloud_function(
    name = "gcloud_function",
    env_vars_file = "env.yaml",
    function_name = "handleStackdriverEvent",
    npm_package = ":package",
)
