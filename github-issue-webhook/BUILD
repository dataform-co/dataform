package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "github-issue-webhook",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/github-issue-webhook",
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
    deps = [":github-issue-webhook"],
)

load(":gcloud_function.bzl", "nodejs_gcloud_function")

nodejs_gcloud_function(
    name = "gcloud_function",
    function_name = "handleStackdriverEvent",
    npm_package = ":package",
)
